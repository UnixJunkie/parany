
open Printf
module Fn = Filename

let debug = ref false

let core_pinning = ref false (* OFF by default, because of multi-users *)

let enable_core_pinning () =
  core_pinning := true

let disable_core_pinning () =
  core_pinning := false

exception End_of_input

module Send = struct

  let unmarshal_from_file fn =
    (* mmap -> O_RDWR *)
    let fd = Unix.(openfile fn [O_RDWR] 0) in
    let a =
      Bigarray.array1_of_genarray
        (Unix.map_file fd Bigarray.char Bigarray.c_layout true [|-1|]) in
    let res = Bytearray.unmarshal a 0 in
    Unix.close fd;
    res

  let marshal_to_file fn v =
    (* mmap -> O_RDWR *)
    let fd = Unix.(openfile fn [O_RDWR] 0o600) in
    let s = Marshal.to_string v [Marshal.No_sharing] in
    ignore(Bytearray.mmap_of_string fd s);
    Unix.close fd

  let raw_send sock str =
    Sendmsg.send sock (Bytes.unsafe_of_string str) 0 (String.length str)

  let send queue to_send =
    let fn = Fn.temp_file "parany_" "" in
    marshal_to_file fn to_send;
    ignore(raw_send queue fn)

  let raw_receive sock buff =
    let count, none = Sendmsg.recv sock buff 0 (Bytes.length buff) in
    assert(none = None);
    (count, Bytes.sub_string buff 0 count)

  let receive queue buff =
    let count, fn = raw_receive queue buff in
    if fn = "EOF" then
      raise End_of_input
    else
      begin
        (* no message should have length 0 *)
        (* the buffer should never be completely filled by the message
         * because the message is just a rather short filename *)
        assert(count > 0 && count < (Bytes.length buff));
        let res = unmarshal_from_file fn in
        Sys.remove fn;
        res
      end

end

(* feeder process main loop *)
let feed_them_all csize ncores demux queue =
  let pid = Unix.getpid () in
  printf "feeder %d: started\n%!" pid;
  try
    while true do
      let to_send = ref [] in
      for _ = 1 to csize do
        to_send := (demux ()) :: !to_send
      done;
      Send.send queue !to_send
    done;
    assert(false)
  with End_of_input ->
    begin
      (* send one EOF to each worker *)
      for _i = 1 to ncores do
        ignore(Send.raw_send queue "EOF")
      done;
      printf "feeder %d: finished\n%!" pid;
      Unix.close queue
    end

(* worker process loop *)
let go_to_work jobs_queue work results_queue =
  let pid = Unix.getpid () in
  printf "worker %d: started\n%!" pid;
  try
    let buff = Bytes.create 80 in
    while true do
      let xs = Send.receive jobs_queue buff in
      let ys = List.rev_map work xs in
      printf "worker %d: did one\n%!" pid;
      Send.send results_queue ys
    done;
  with End_of_input ->
    begin
      (* tell collector to stop *)
      printf "worker %d: finished\n%!" pid;
      ignore(Send.raw_send results_queue "EOF");
      Unix.close results_queue
    end

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> let () = f () in exit 0
  | _pid -> ()

let run ~verbose ~csize ~nprocs ~demux ~work ~mux =
  debug := verbose;
  if nprocs <= 1 then
    (* sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    begin
      assert(csize >= 1);
      let max_cores = Cpu.numcores () in
      assert(nprocs <= max_cores);
      (* parallel version *)
      let pid = Unix.getpid () in
      printf "father %d: started\n%!" pid;
      (* create queues *)
      let jobs_in, jobs_out = Unix.(socketpair PF_UNIX SOCK_DGRAM 0) in
      let res_in, res_out = Unix.(socketpair PF_UNIX SOCK_DGRAM 0) in
      (* start feeder *)
      printf "father %d: starting feeder\n%!" pid;
      Gc.compact (); (* like parmap: reclaim memory prior to forking *)
      fork_out (fun () -> feed_them_all csize nprocs demux jobs_in);
      (* start workers *)
      for worker_rank = 0 to nprocs - 1 do
        printf "father %d: starting a worker\n%!" pid;
        fork_out (fun () ->
            if !core_pinning then Cpu.setcore worker_rank;
            go_to_work jobs_out work res_in
          )
      done;
      (* collect results *)
      let finished = ref 0 in
      let buff = Bytes.create 80 in
      while !finished < nprocs do
        try
          while true do
            let xs = Send.receive res_out buff in
            printf "father %d: collecting one\n%!" pid;
            List.iter mux xs
          done
        with End_of_input ->
          incr finished
      done;
      printf "father %d: finished\n%!" pid;
      (* free resources *)
      List.iter (Unix.close) [jobs_in; jobs_out; res_in; res_out]
    end

(* Wrapper for near-compatibility with Parmap *)
module Parmap = struct

  let tail_rec_map f l =
    List.rev (List.rev_map f l)

  let parmap ~ncores ?(csize = 1) f l =
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
      run ~verbose:false ~csize ~nprocs:ncores ~demux ~work:f ~mux;
      !output

  let pariter ~ncores ?(csize = 1) f l =
    if ncores <= 1 then List.iter f l
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      (* parallel work *)
      run ~verbose:false ~csize ~nprocs:ncores ~demux ~work:f ~mux:ignore

  let parfold ~ncores ?(csize = 1) f g init l =
    if ncores <= 1 then List.fold_left g init (tail_rec_map f l)
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      let output = ref init in
      let mux x =
        output := g !output x in
      (* parallel work *)
      run ~verbose:false ~csize ~nprocs:ncores ~demux ~work:f ~mux;
      !output
end
