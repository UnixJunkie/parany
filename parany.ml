
open Printf

type 'a message =
  | Msg of 'a
  | Stop of int (* Tell consumer nothing more will come.
                   The int param is just here to make sure
                   this value is boxed (so that ocamlnet
                   can (de)serialize it properly from/to shm). *)

(* unnamed posix semaphore *)
module Sem = struct

  open Posix_semaphore

  type 'a t = 'a semaphore

  exception Sem_except of string

  let unwrap = function
    | Result.Error (`EUnix err) -> raise (Sem_except (Unix.error_message err))
    | Result.Ok x -> x

  (* value = initial number of tokens *)
  let create_exn value =
    unwrap (sem_init value)

  let destroy_exn sem =
    unwrap (sem_destroy sem)

  (* add 1 to sem *)
  let post_exn sem =
    unwrap (sem_post sem)

  let wait_exn sem =
    unwrap (sem_wait sem)

  let try_wait sem =
    match sem_trywait sem with
    | Result.Error _ -> false (* did not get sem *)
    | Result.Ok () -> true (* got it *)

end

type ('a, 'b) t = { id: Netmcore.res_id;
                    q: ('a, unit) Netmcore_queue.squeue;
                    blocked_pushers: 'b;
                    nb_elts: 'b}

(* queue for parallel processing *)
module Pqueue = struct

  let create () =
    let one_GB = 1024 * 1024 * 1024 in
    let mem_pool_id = Netmcore_mempool.create_mempool one_GB in
    let queue = Netmcore_queue.create mem_pool_id () in
    { id = mem_pool_id;
      q = queue;
      blocked_pushers = Sem.create_exn 0;
      nb_elts = Sem.create_exn 0 }

  let destroy q =
    Netmcore_mempool.unlink_mempool q.id;
    Sem.destroy_exn q.blocked_pushers;
    Sem.destroy_exn q.nb_elts

  (* WARNING: blocking in case queue is full *)
  let rec push queue (x: 'a message): unit =
    try
      Netmcore_queue.push x queue.q; (* push elt *)
      Sem.post_exn queue.nb_elts (* incr nb. elt *)
    with Netmcore_mempool.Out_of_pool_memory ->
      (* queue is full *)
      Sem.wait_exn queue.blocked_pushers;
      push queue x

  (* WARNING: blocking in case queue is empty *)
  let worker_process_one queue (f: 'a message -> unit): unit =
    (* Netmcore_queue.pop_p avoids data copy out of the shared heap *)
    Sem.wait_exn queue.nb_elts; (* wait until queue is non empty *)
    if Sem.try_wait queue.blocked_pushers then
      (* we got the sem; someone is locked waiting for us to pop one elt *)
      (Netmcore_queue.pop_p queue.q f;
       Sem.post_exn queue.blocked_pushers)
    else (* no one is blocked because of full queue *)
      Netmcore_queue.pop_p queue.q f

  (* WARNING: blocking in case queue is empty *)
  let collector_process_one queue (f: 'a message -> unit): unit =
    (* Netmcore_queue.pop_c: the collector does data copy
       out of the shared heap into normal memory
       so that end users of the library are safer *)
    Sem.wait_exn queue.nb_elts; (* wait until queue is non empty *)
    if Sem.try_wait queue.blocked_pushers then
      (* we got the sem; someone is locked waiting for us to pop one elt *)
      (f (Netmcore_queue.pop_c queue.q);
       Sem.post_exn queue.blocked_pushers)
    else (* no one is blocked because of full queue *)
      f (Netmcore_queue.pop_c queue.q)

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
    let jobs_queue = Pqueue.create () in
    let results_queue = Pqueue.create () in
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
    Pqueue.destroy jobs_queue;
    Pqueue.destroy results_queue
