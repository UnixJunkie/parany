
open Printf

type 'a message =
  | Msg of 'a
  | Stop of int (* Tell consumer nothing more will come.
                   The int param is just here to make sure
                   this value is boxed (so that ocamlnet
                   can (de)serialize it properly from/to shm). *)

module Sem = struct

  let count = ref 0

  open Netsys_posix

  type t = { name: string; (* name in filesystem *)
             sem: named_semaphore } (* actual semaphore *)

  let constr name sem =
    { name; sem }

  let create value =
    let name =
      sprintf "/libparany_p%d_s%d" !count (Unix.getpid ()) in
    incr count;
    (* printf "sem_name: %s\n%!" name; *)
    let sem = sem_open name [SEM_O_CREAT] 0o600 value in
    constr name sem

  let unlink x =
    sem_unlink x.name

  (* add 1 to sem *)
  let post x =
    sem_post x.sem

  (* blocking wait *)
  let wait x =
    sem_wait x.sem SEM_WAIT_BLOCK

  let try_wait x =
    try
      sem_wait x.sem SEM_WAIT_NONBLOCK;
      true (* got sem *)
    with
    | Unix.Unix_error (Unix.EAGAIN, _, _) -> false (* did not get sem *)
    | exn -> raise exn

end

type 'a t = { id: Netmcore.res_id;
              q: ('a, unit) Netmcore_queue.squeue;
              blocked_pushers: Sem.t;
              nb_elts: Sem.t }

(* queue for parallel processing *)
module Pqueue = struct

  let create () =
    let one_GB = 1024 * 1024 * 1024 in
    let mem_pool_id = Netmcore_mempool.create_mempool one_GB in
    let queue = Netmcore_queue.create mem_pool_id () in
    { id = mem_pool_id;
      q = queue;
      blocked_pushers = Sem.create 0;
      nb_elts = Sem.create 0 }

  let destroy q =
    Netmcore_mempool.unlink_mempool q.id;
    (* Sem.close q.blocked_pushers; *) (* makes the program crash !!! *)
    Sem.unlink q.blocked_pushers;
    (* Sem.close q.nb_elts; *) (* makes the program crash !!! *)
    Sem.unlink q.nb_elts

  (* WARNING: blocking in case queue is full *)
  let rec push queue (x: 'a message): unit =
    try
      Netmcore_queue.push x queue.q; (* push elt *)
      Sem.post queue.nb_elts (* incr nb. elt *)
    with Netmcore_mempool.Out_of_pool_memory ->
      (* queue is full *)
      Sem.wait queue.blocked_pushers;
      push queue x

  (* WARNING: blocking in case queue is empty *)
  let worker_process_one queue (f: 'a message -> unit): unit =
    (* Netmcore_queue.pop_p avoids data copy out of the shared heap *)
    Sem.wait queue.nb_elts; (* wait until queue is non empty *)
    if Sem.try_wait queue.blocked_pushers then
      (* we got the sem; someone is locked waiting for us to pop one elt *)
      (Netmcore_queue.pop_p queue.q f;
       Sem.post queue.blocked_pushers)
    else (* no one is blocked because of full queue *)
      Netmcore_queue.pop_p queue.q f

  (* WARNING: blocking in case queue is empty *)
  let collector_process_one queue (f: 'a message -> unit): unit =
    (* Netmcore_queue.pop_c: the collector does data copy
       out of the shared heap into normal memory
       so that end users of the library are safer *)
    Sem.wait queue.nb_elts; (* wait until queue is non empty *)
    if Sem.try_wait queue.blocked_pushers then
      (* we got the sem; someone is locked waiting for us to pop one elt *)
      (f (Netmcore_queue.pop_c queue.q);
       Sem.post queue.blocked_pushers)
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
    Gc.compact (); (* like parmap: reclaim memory prior to forking *)
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
