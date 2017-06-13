
module Ht = Hashtbl

module L = struct
  include List
  (* missing List.create from the stdlib, !!! NOT TAIL REC !!! *)
  let rec create n x =
    if n = 1 then [x]
    else x :: (create (n - 1) x)
end

(* must be thrown by the demux function once there is no more to read *)
exception End_of_input

(* where to write to *)
let pipe_entrance p =
  snd p

(* where to read from *)
let pipe_exit p =
  fst p

let create_pipes nprocs =
  assert(nprocs > 1);
  L.map Unix.pipe (L.create nprocs ())

let close_all fds =
  L.iter Unix.close fds

(* feeder process main loop *)
let feed_them_all in_pipes demux =
  let no_timeout = -1.0 in
  try
    (* FBR: I should use in_chans here too *)
    while true do
      (* who is waiting for some work (pull scheduler) *)
      (* Log.info "feeder: waiting for workers"; *)
      let _, accepting, _ = Unix.select [] in_pipes [] no_timeout in
      L.iter (fun pipe_input ->
          (* Log.info "feeder: creating job"; *)
          let x = demux () in
          let buff = Marshal.(to_bytes x [No_sharing]) in
          let to_write = Bytes.length buff in
          let written = Unix.single_write pipe_input buff 0 to_write in
          if written <> to_write then
            failwith (Printf.sprintf "feeder: %d <> %d" written to_write);
          (* Log.info "feeder: job sent" *)
        ) accepting
    done
  with End_of_input ->
    (* close pipes to workers *)
    close_all in_pipes
    (* Log.info "feeder: closed workers inputs" *)

let work rank fdin f fdout =
  let input = Unix.in_channel_of_descr fdin in
  let output = Unix.out_channel_of_descr fdout in
  try
    while true do
      (* Log.info "work %d: waiting job" rank; *)
      let x = Marshal.from_channel input in
      (* Log.info "work %d: got one" rank; *)
      let y = f x in
      Marshal.(to_channel output y [No_sharing]);
      (* Log.info "work %d: sent one" rank; *)
    done
  with End_of_file ->
    (* notify parent process that no more results will come out
       from the other end of this pipe *)
    ((* Log.info "work %d: job done" rank; *)
     close_in input;
     close_out output)

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> (f (); exit 0)
  | _pid -> ()

(* FBR: add csize with default to 1 *)
let run (nprocs: int) (demux: unit -> 'a) (f: 'a -> 'b) (mux: 'b -> unit)
  : unit =
  if nprocs = 1 then (* sequential version *)
    try
      while true do
        mux (f (demux ()))
      done
    with End_of_input -> ()
  else (* parallel version *)
    let input_pipes = create_pipes nprocs in (* to feed workers *)
    assert(L.length input_pipes = nprocs);
    (* start feeder process *)
    let worker_mouths = L.map pipe_entrance input_pipes in
    (* Log.info "run: forking feeder"; *)
    fork_out (fun () -> feed_them_all worker_mouths demux);
    (* no one else is supposed to write to them later *)
    close_all worker_mouths;
    let output_pipes = create_pipes nprocs in (* to gather results *)
    let fdins = L.map pipe_exit input_pipes in
    let fdouts = L.map pipe_entrance output_pipes in
    let fdin_fdouts = L.combine fdins fdouts in
    (* create workers *)
    L.iteri (fun i (fdin, fdout) ->
        (* Log.info "run: forking worker"; *)
        fork_out (fun () -> work i fdin f fdout);
        (* no one else is supposed to read/write in those *)
        Unix.close fdin;
        Unix.close fdout
      ) fdin_fdouts;
    (* gather all results *)
    let result_pipes = ref (L.map pipe_exit output_pipes) in
    let finished = ref 0 in
    let no_timeout = -1.0 in
    let fd2inchan = Ht.create nprocs in
    L.iter (fun fd ->
        Ht.add fd2inchan fd (Unix.in_channel_of_descr fd)
      ) !result_pipes;
    while !finished <> nprocs do
      (* Log.info "run: waiting results"; *)
      let giving, _, _ = Unix.select !result_pipes [] [] no_timeout in
      L.iter (fun fdin ->
          (* retrieve in_chan corresponding to that fd *)
          let input = Ht.find fd2inchan fdin in
          try
            let x = Marshal.from_channel input in
            (* Log.info "run: got one result"; *)
            mux x
          with End_of_file ->
            ((* Log.info "run: one worker finished"; *)
             Unix.close fdin;
             (* update list of fds to watch *)
             result_pipes := L.filter (fun fd -> fd <> fdin) !result_pipes;
             incr finished)
        ) giving
    done
