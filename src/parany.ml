
open Printf
module Fn = Filename
module Ht = Hashtbl

exception End_of_input

module Shm = struct

  let init () =
    Unix.(socketpair PF_UNIX SOCK_DGRAM 0)

  let unmarshal_from_file fn =
    let input = open_in_bin fn in
    let res = Marshal.from_channel input in
    close_in input;
    res

  let marshal_to_file fn v =
    let out = open_out_bin fn in
    Marshal.to_channel out v [Marshal.No_sharing];
    close_out out

  let rec send_loop sock buff n =
    try
      let sent = Unix.send sock buff 0 n [] in
      assert(sent = n)
    with Unix.Unix_error(ENOBUFS, _, _) ->
      (* send on a UDP socket never blocks on Mac OS X
         and probably several of the BSDs *)
      (* eprintf "sleep\n%!"; *)
      let _ = Unix.select [] [] [] 0.001 in (* wait *)
      (* We should use nanosleep for precision, if only it
         was provided by the Unix module... *)
      send_loop sock buff n

  let raw_send sock str =
    let n = String.length str in
    let buff = Bytes.unsafe_of_string str in
    send_loop sock buff n

  let send fn queue to_send =
    marshal_to_file fn to_send;
    raw_send queue fn

  let raw_receive sock buff =
    let n = Bytes.length buff in
    let received = Unix.recv sock buff 0 n [] in
    assert(received > 0);
    Bytes.sub_string buff 0 received

  let receive queue buff =
    let fn = raw_receive queue buff in
    if fn = "EOF" then
      raise End_of_input
    else
      let res = unmarshal_from_file fn in
      Sys.remove fn;
      res

end

(* feeder process loop *)
let feed_them_all csize ncores demux queue =
  (* let pid = Unix.getpid () in
   * eprintf "feeder(%d) started\n%!" pid; *)
  let in_count = ref 0 in
  let prfx = Filename.temp_file "iparany_" "" in
  let to_send = ref [] in
  try
    while true do
      for _ = 1 to csize do
        to_send := (demux ()) :: !to_send
      done;
      let fn = sprintf "%s_%d" prfx !in_count in
      Shm.send fn queue !to_send;
      (* eprintf "feeder(%d) sent one\n%!" pid; *)
      to_send := [];
      incr in_count
    done
  with End_of_input ->
    begin
      (* if needed, send remaining jobs (< csize) *)
      (if !to_send <> [] then
         let fn = sprintf "%s_%d" prfx !in_count in
         Shm.send fn queue !to_send);
      (* send an EOF to each worker *)
      for _ = 1 to ncores do
        Shm.raw_send queue "EOF"
      done;
      (* eprintf "feeder(%d) finished\n%!" pid; *)
      Sys.remove prfx;
      Unix.close queue
    end

(* worker process loop *)
let go_to_work prfx jobs_queue work results_queue =
  (* let pid = Unix.getpid () in
   * eprintf "worker(%d) started\n%!" pid; *)
  try
    let out_count = ref 0 in
    let buff = Bytes.create 80 in
    while true do
      let xs = Shm.receive jobs_queue buff in
      let ys = List.rev_map work xs in
      (* eprintf "worker(%d) did one\n%!" pid; *)
      let fn = sprintf "%s_%d" prfx !out_count in
      Shm.send fn results_queue ys;
      incr out_count
    done
  with End_of_input ->
    (* resource cleanup was moved to an at_exit-registered function,
       so that cleanup is done even in the case of an uncaught exception
       and the muxer doesn't enter an infinite loop *)
    ()

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> let () = f () in exit 0
  | _pid -> ()

(* demux and index items *)
let idemux (demux: unit -> 'a) =
  let demux_count = ref 0 in
  function () ->
    let res = (!demux_count, demux ()) in
    incr demux_count;
    res

(* work ignoring item index *)
let iwork (work: 'a -> 'b) ((i, x): int * 'a): int * 'b =
  (i, work x)

(* mux items in the right order *)
let imux (mux: 'b -> unit) =
  let mux_count = ref 0 in
  (* weak type variable avoidance *)
  let wait_list = Ht.create 11 in
  function (i, res) ->
    if !mux_count = i then
      begin
        (* unpile as much as possible *)
        mux res;
        incr mux_count;
        if Ht.length wait_list > 0 then
          try
            while true do
              let next = Ht.find wait_list !mux_count in
              Ht.remove wait_list !mux_count;
              mux next;
              incr mux_count
            done
          with Not_found -> () (* no more or index hole *)
      end
    else
      (* put somewhere into the pile *)
      Ht.add wait_list i res

let run ?(init = fun (_rank: int) -> ()) ?(finalize = fun () -> ())
    ?(preserve = false) ?(core_pin = false) ?(csize = 1) nprocs
    ~demux ~work ~mux =
  (* the (type a b) annotation unfortunately implies OCaml >= 4.03.0 *)
  let demux_work_mux (type a b)
      ~(demux: unit -> a) ~(work: a -> b) ~(mux: b -> unit): unit =
    (* create queues *)
    let jobs_in, jobs_out = Shm.init () in
    let res_in, res_out = Shm.init () in
    (* start feeder *)
    (* eprintf "father(%d) starting feeder\n%!" pid; *)
    flush_all (); (* prevent duplicated I/O *)
    Gc.compact (); (* like parmap: reclaim memory prior to forking *)
    fork_out (fun () -> feed_them_all csize nprocs demux jobs_in);
    (* start workers *)
    for worker_rank = 0 to nprocs - 1 do
      (* eprintf "father(%d) starting a worker\n%!" pid; *)
      fork_out (fun () ->
          init worker_rank; (* per-process optional setup *)
          at_exit finalize; (* register optional finalize fun *)
          (* parmap also does core pinning _after_ having called
             the per-process init function *)
          if core_pin then Cpu.setcore worker_rank;
          let prfx = Filename.temp_file "oparany_" "" in
          at_exit (fun () ->
              (* tell collector to stop *)
              (* eprintf "worker(%d) finished\n%!" pid; *)
              Shm.raw_send res_in "EOF";
              Unix.close res_in;
              Sys.remove prfx
            );
          go_to_work prfx jobs_out work res_in
        )
    done;
    (* collect results *)
    let finished = ref 0 in
    let buff = Bytes.create 80 in
    while !finished < nprocs do
      try
        while true do
          let xs = Shm.receive res_out buff in
          (* eprintf "father(%d) collecting one\n%!" pid; *)
          List.iter mux xs
        done
      with End_of_input -> incr finished
    done;
    (* free resources *)
    List.iter Unix.close [jobs_in; jobs_out; res_in; res_out]
  in
  if nprocs <= 1 then
    (* sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    begin
      (* parallel version *)
      assert(csize >= 1);
      let max_cores = Cpu.numcores () in
      assert(nprocs <= max_cores);
      (* let pid = Unix.getpid () in
       * eprintf "father(%d) started\n%!" pid; *)
      (if preserve then
         (* In some cases, it is necessary for the user to preserve the
            input order in the output. In this case, we still compute things
            potentially out of order (for parallelization efficiency);
            but we will order back the results in input order
            (for user's convenience) *)
         demux_work_mux
           ~demux:(idemux demux) ~work:(iwork work) ~mux:(imux mux)
       else
         (* by default, to maximize parallel efficiency we don't care about the
            order in which jobs are computed. *)
         demux_work_mux ~demux ~work ~mux
      );
      (* eprintf "father(%d) finished\n%!" pid; *)
    end

(* Wrapper for near-compatibility with Parmap *)
module Parmap = struct

  let tail_rec_map f l =
    List.rev (List.rev_map f l)

  let parmap ?(init = fun (_rank: int) -> ()) ?(finalize = fun () -> ())
      ?(preserve = false) ?(core_pin = false) ?(csize = 1) ncores f l =
    if ncores <= 1 then tail_rec_map f l
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      let output = ref [] in
      let mux x =
        output := x :: !output in
      (* parallel work *)
      run ~init ~finalize ~preserve ~core_pin ~csize ncores ~demux ~work:f ~mux;
      !output

  let pariter ?(init = fun (_rank: int) -> ()) ?(finalize = fun () -> ())
      ?(preserve = false) ?(core_pin = false) ?(csize = 1) ncores f l =
    if ncores <= 1 then List.iter f l
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      (* parallel work *)
      run ~init ~finalize ~preserve ~core_pin ~csize ncores ~demux ~work:f ~mux:ignore

  let parfold ?(init = fun (_rank: int) -> ()) ?(finalize = fun () -> ())
      ?(preserve = false) ?(core_pin = false) ?(csize = 1) ncores
      f g init_acc l =
    if ncores <= 1 then
      List.fold_left (fun acc x -> g acc (f x)) init_acc l
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      let output = ref init_acc in
      let mux x =
        output := g !output x in
      (* parallel work *)
      run ~init ~finalize ~preserve ~core_pin ~csize ncores ~demux ~work:f ~mux;
      !output

  (* let parfold_compat
   *     ?(init = fun (_rank: int) -> ()) ?(finalize = fun () -> ())
   *     ?(ncores: int option) ?(chunksize: int option) (f: 'a -> 'b -> 'b)
   *     (l: 'a list) (init_acc: 'b) (acc_fun: 'b -> 'b -> 'b): 'b =
   *   let nprocs = match ncores with
   *     | None -> 1 (\* if the user doesn't know the number of cores to use,
   *                    we don't know better *\)
   *     | Some x -> x in
   *   let csize = match chunksize with
   *     | None -> 1
   *     | Some x -> x in
   *   if nprocs <= 1 then
   *     List.fold_left (fun acc x -> f x acc) init_acc l
   *   else
   *     let input = ref l in
   *     let demux () = match !input with
   *       | [] -> raise End_of_input
   *       | _ ->
   *         let this_chunk, rest = BatList.takedrop csize !input in
   *         input := rest;
   *         this_chunk in
   *     let work xs =
   *       List.fold_left (fun acc x -> f x acc) init_acc xs in
   *     let output = ref init_acc in
   *     let mux x =
   *       output := acc_fun !output x in
   *     (\* parallel work *\)
   *     run ~init ~finalize
   *       (\* leave csize=1 bellow *\)
   *       ~preserve:false ~core_pin:false ~csize:1 nprocs ~demux ~work ~mux;
   *     !output *)

end
