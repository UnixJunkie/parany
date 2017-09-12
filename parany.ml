
open Printf

type 'a t = { id: Netmcore.res_id;
              q: ('a, unit) Netmcore_queue.squeue }

(* queue for parallel processing *)
module Pqueue = struct

  let create () =
    let one_GB = 1024 * 1024 * 1024 in
    let mem_pool_id = Netmcore_mempool.create_mempool one_GB in
    let queue = Netmcore_queue.create mem_pool_id () in
    { id = mem_pool_id;
      q = queue }

  let destroy q =
    Netmcore_mempool.unlink_mempool q.id

  (* WARNING: blocking in case queue is full *)
  let rec push queue (x: 'a list): unit =
    try Netmcore_queue.push x queue.q (* push elt *)
    with Netmcore_mempool.Out_of_pool_memory ->
      (* queue is full *)
      begin
        eprintf "warn: Pqueue.push: full\n%!";
        Unix.sleepf 0.001;
        push queue x
      end

  let rec worker_process_one queue (f: 'a list -> unit): unit =
    (* Netmcore_queue.pop_p avoids data copy out of the shared heap *)
    try Netmcore_queue.pop_p queue.q f
    with Netmcore_queue.Empty ->
      begin
        eprintf "warn: Pqueue.worker_process_one: empty\n%!";
        Unix.sleepf 0.001;
        worker_process_one queue f
      end

  let rec collector_process_one queue (f: 'a list -> unit): unit =
    (* Netmcore_queue.pop_c: the collector does data copy
       out of the shared heap into normal memory
       so that end users of the library are safer *)
    try f (Netmcore_queue.pop_c queue.q)
    with Netmcore_queue.Empty ->
      begin
        eprintf "warn: Pqueue.collector_process_one: empty\n%!";
        Unix.sleepf 0.001;
        collector_process_one queue f
      end

end

exception End_of_input

(* feeder process main loop *)
let feed_them_all csize nprocs demux queue =
  (* let pid = Unix.getpid () in *)
  (* printf "feeder %d: started\n%!" pid; *)
  let to_send = ref [] in
  try
    while true do
      for i = 1 to csize do
        let x = demux () in
        to_send := x :: !to_send
      done;
      Pqueue.push queue !to_send;
      to_send := []
    done
  with End_of_input ->
    (if !to_send <> [] then Pqueue.push queue !to_send;
     (* tell workers to stop *)
     (* printf "feeder %d: telling workers to stop\n%!" pid; *)
     for i = 1 to nprocs do
       Pqueue.push queue []
     done)

(* worker process loop *)
let go_to_work jobs_queue work results_queue =
  (* let pid = Unix.getpid () in *)
  (* printf "worker %d: started\n%!" pid; *)
  let finished = ref false in
  while not !finished do
    Pqueue.worker_process_one jobs_queue (function
        | [] -> finished := true
        | xs ->
          let ys = List.rev_map work xs in
          (* printf "worker %d: did one\n%!" pid; *)
          Pqueue.push results_queue ys
      )
  done;
  (* tell collector to stop *)
  (* printf "worker %d: I'm done\n%!" pid; *)
  Pqueue.push results_queue []

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> let () = f () in exit 0
  | _pid -> ()

let run ~csize ~nprocs ~demux ~work ~mux =
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
      (* parallel version *)
      (* let pid = Unix.getpid () in *)
      (* printf "father %d: started\n%!" pid; *)
      (* create queues *)
      let jobs_queue = Pqueue.create () in
      let results_queue = Pqueue.create () in
      (* start feeder *)
      (* printf "father %d: starting feeder\n%!" pid; *)
      Gc.compact (); (* like parmap: reclaim memory prior to forking *)
      fork_out (fun () -> feed_them_all csize nprocs demux jobs_queue);
      (* start workers *)
      for i = 1 to nprocs do
        (* printf "father %d: starting a worker\n%!" pid; *)
        fork_out (fun () -> go_to_work jobs_queue work results_queue)
      done;
      (* collect results *)
      let nb_finished = ref 0 in
      while !nb_finished < nprocs do
        Pqueue.collector_process_one results_queue (fun msg ->
            match msg with
            | [] -> incr nb_finished
            | xs ->
              (* printf "father %d: collecting one\n%!" pid; *)
              List.iter mux xs
          )
      done;
      (* free resources *)
      Pqueue.destroy jobs_queue;
      Pqueue.destroy results_queue
    end
