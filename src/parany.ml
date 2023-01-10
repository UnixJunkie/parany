
module A = Array
module Chan = Domainslib.Chan
module Ht = Hashtbl

exception End_of_input

let id2rank =
  (* +1 because the always running initial thread has id 0 *)
  Array.make (1 + Domain.recommended_domain_count()) (-1)

(* only workers are supposed to call this *)
let get_rank () =
  let my_thread_id = (Domain.self () :> int) in
  id2rank.(my_thread_id)

let worker_loop jobs results work =
  (* let start_rank = get_rank () in *)
  let rec loop () =
    match Chan.recv jobs with
    | [||] ->
      (* let end_rank = get_rank () in *)
      (* assert(start_rank = end_rank); (\* not supposed to change *\) *)
      (* signal muxer thread *)
      Chan.send results [||]
    | arr ->
      begin
        let res = Array.map work arr in
        Chan.send results res;
        loop ()
      end
  in
  loop ()

let demuxer_loop nprocs jobs csize demux =
  let rec loop () =
    let to_send = ref [] in
    try
      for _i = 1 to csize do
        to_send := (demux ()) :: !to_send
      done;
      let xs = Array.of_list (List.rev !to_send) in
      Chan.send jobs xs;
      loop ()
    with End_of_input ->
      begin
        if !to_send <> [] then
          begin
            let xs = Array.of_list (List.rev !to_send) in
            Chan.send jobs xs
          end;
        for _i = 1 to nprocs do
          (* signal each worker *)
          Chan.send jobs [||]
        done
      end in
  loop ()

let muxer_loop nprocs results mux =
  let finished = ref 0 in
  let rec loop () =
    match Chan.recv results with
    | [||] -> (* one worker finished *)
      begin
        incr finished;
        if !finished = nprocs then ()
        else loop ()
      end
    | xs ->
      begin
        Array.iter mux xs;
        loop ()
      end
  in
  loop ()

(* demux and index items *)
let idemux (demux: unit -> 'a) =
  let demux_count = ref 0 in
  function () ->
    let res = (!demux_count, demux ()) in
    incr demux_count;
    res

(* work, ignoring item index *)
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

let run ?(preserve = false) ?(csize = 1) (nprocs: int) ~demux ~work ~mux =
  if nprocs <= 1 then
    (* no overhead sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    begin
      if preserve then
        (* nprocs workers + demuxer thread + muxer thread *)
        let jobs = Chan.make_bounded (50 * nprocs) in
        let results = Chan.make_bounded (50 * nprocs) in
        (* launch the workers *)
        let workers =
          Array.init nprocs (fun rank ->
              Domain.spawn (fun () ->
                  let my_thread_id = (Domain.self () :> int) in
                  id2rank.(my_thread_id) <- rank;
                  (* assert(rank = get_rank ()); (\* extra cautious *\) *)
                  worker_loop jobs results (iwork work)
                )
            ) in
        (* launch the demuxer *)
        let demuxer = Domain.spawn (fun () ->
            demuxer_loop nprocs jobs csize (idemux demux)
          ) in
        (* use the current thread to collect results (muxer) *)
        muxer_loop nprocs results (imux mux);
        (* wait all threads *)
        Domain.join demuxer;
        Array.iter Domain.join workers
      else
        (* WARNING: serious code duplication below... *)
        (* nprocs workers + demuxer thread + muxer thread *)
        let jobs = Chan.make_bounded (50 * nprocs) in
        let results = Chan.make_bounded (50 * nprocs) in
        (* launch the workers *)
        let workers =
          Array.init nprocs (fun rank ->
              Domain.spawn (fun () ->
                  let my_thread_id = (Domain.self () :> int) in
                  id2rank.(my_thread_id) <- rank;
                  (* assert(rank = get_rank ()); (\* extra cautious *\) *)
                  worker_loop jobs results work
                )
            ) in
        (* launch the demuxer *)
        let demuxer = Domain.spawn (fun () ->
            demuxer_loop nprocs jobs csize demux
          ) in
        (* use the current thread to collect results (muxer) *)
        muxer_loop nprocs results mux;
        (* wait all threads *)
        Domain.join demuxer;
        Array.iter Domain.join workers
    end

(* Wrapper for near-compatibility with Parmap *)
module Parmap = struct

  let tail_rec_map f l =
    List.rev (List.rev_map f l)

  let tail_rec_mapi f l =
    let i = ref 0 in
    let res =
      List.rev_map (fun x ->
          let j = !i in
          let y = f j x in
          incr i;
          y
        ) l in
    List.rev res

  let parmap
      ?(preserve = false) ?(csize = 1) ncores f l =
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
      run ~preserve ~csize ncores ~demux ~work:f ~mux;
      if preserve then
        List.rev !output
      else
        !output

  let parmapi
      ?(preserve = false) ?(csize = 1) ncores f l =
    if ncores <= 1 then tail_rec_mapi f l
    else
      let input = ref l in
      let i = ref 0 in
      let demux () =
        match !input with
        | [] -> raise End_of_input
        | x :: xs ->
          begin
            let j = !i in
            input := xs;
            let res = (j, x) in
            incr i;
            res
          end in
      let output = ref [] in
      let f' (i, x) = f i x in
      let mux x =
        output := x :: !output in
      (* parallel work *)
      run ~preserve ~csize ncores ~demux ~work:f' ~mux;
      if preserve then
        List.rev !output
      else
        !output

  let pariter
      ?(preserve = false) ?(csize = 1) ncores f l =
    if ncores <= 1 then List.iter f l
    else
      let input = ref l in
      let demux () = match !input with
        | [] -> raise End_of_input
        | x :: xs -> (input := xs; x) in
      (* parallel work *)
      run ~preserve ~csize ncores ~demux ~work:f ~mux:ignore

  let parfold
      ?(preserve = false)
      ?(csize = 1) ncores
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
      run ~preserve ~csize ncores ~demux ~work:f ~mux;
      !output

  (* preserves array input order *)
  let array_parmap ncores f init_acc a =
    let n = A.length a in
    let res = A.make n init_acc in
    run
      ~preserve:false (* input-order is preserved explicitely below *)
      ~csize:1 ncores
      ~demux:(
        let in_count = ref 0 in
        fun () ->
          if !in_count = n then
            raise End_of_input
          else
            let i = !in_count in
            incr in_count;
            i)
      ~work:(fun i -> (i, f (A.unsafe_get a i)))
      ~mux:(fun (i, y) -> A.unsafe_set res i y);
    res

end
