
open Printf

type 'a message =
  | Msg of 'a
  | Stop of int (* Tell consumer nothing more will come.
                   The int param is just here to make sure
                   this value is boxed (so that ocamlnet
                   can (de)serialize it properly from/to shm). *)

(* queue for parallel processing *)
module Pqueue = struct

  let create () =
    let one_GB = 1024 * 1024 * 1024 in
    let mem_pool_id = Netmcore_mempool.create_mempool one_GB in
    let queue = Netmcore_queue.create mem_pool_id () in
    (mem_pool_id, queue)

  let destroy mem_pool_id =
    Netmcore_mempool.unlink_mempool mem_pool_id

  (* WARNING: blocking in case the queue is full *)
  let rec push queue (x: 'a message): unit =
    try Netmcore_queue.push x queue
    with Netmcore_mempool.Out_of_pool_memory ->
      (* the queue is full. If there was a smart blocking
         push operation using a semaphore, that would be cool ... *)
      (Unix.sleepf 0.001;
       push queue x)

  (* WARNING: blocking in case the queue is empty *)
  let rec worker_process_one queue (f: 'a message -> unit): unit =
    (* the worker avoids data copy out of the shared heap: pop_p *)
    try Netmcore_queue.pop_p queue f
    with Netmcore_queue.Empty ->
      (* the queue is empty, there should be a blocking *)
      (* pop_p operation ... *)
      (Unix.sleepf 0.001;
       worker_process_one queue f)

  let rec collector_process_one queue (f: 'a message -> unit): unit =
    (* the collector does data copy out of the shared heap
       into normal memory; so that end users of the library are safer: pop_c *)
    try f (Netmcore_queue.pop_c queue)
    with Netmcore_queue.Empty ->
      (* the queue is empty, there should be a blocking *)
      (* pop_c operation ... *)
      (Unix.sleepf 0.001;
       collector_process_one queue f)

end

exception End_of_input

(* feeder process main loop *)
let feed_them_all nprocs demux queue =
  (* let pid = Unix.getpid () in *)
  (* printf "feeder %d: started\n%!" pid; *)
  try
    while true do
      let x = demux () in
      Pqueue.push queue (Msg x)
    done
  with End_of_input ->
    (* tell workers to stop *)
    (* printf "feeder %d: telling workers to stop\n%!" pid; *)
      for i = 1 to nprocs do
        Pqueue.push queue (Stop 1)
      done

(* worker process loop *)
let go_to_work jobs_queue work results_queue =
  (* let pid = Unix.getpid () in *)
  (* printf "worker %d: started\n%!" pid; *)
  let finished = ref false in
  while not !finished do
    Pqueue.worker_process_one jobs_queue (function
        | Stop _ -> finished := true
        | Msg x ->
          let y = work x in
          (* printf "worker %d: did one\n%!" pid; *)
          Pqueue.push results_queue (Msg y)
      )
  done;
  (* tell collector to stop *)
  (* printf "worker %d: I'm done\n%!" pid; *)
  Pqueue.push results_queue (Stop 1)

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> let () = f () in exit 0
  | _pid -> ()

let run ~nprocs ~demux ~work ~mux =
  if nprocs <= 1 then
    (* sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    (* parallel version *)
    (* let pid = Unix.getpid () in *)
    (* printf "father %d: started\n%!" pid; *)
    (* create queues *)
    let jobs_qid, jobs_queue = Pqueue.create () in
    let results_qid, results_queue = Pqueue.create () in
    (* start feeder *)
    (* printf "father %d: starting feeder\n%!" pid; *)
    fork_out (fun () -> feed_them_all nprocs demux jobs_queue);
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
          | Stop _ -> incr nb_finished
          | Msg x ->
            (* printf "father %d: collecting one\n%!" pid; *)
            mux x
        )
    done;
    (* free resources *)
    Pqueue.destroy jobs_qid;
    Pqueue.destroy results_qid
