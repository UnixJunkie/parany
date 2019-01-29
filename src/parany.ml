
module Pr = Printf

let debug = ref false

type 'a t = { id: Netmcore.res_id;
              name: string;
              q: ('a, unit) Netmcore_queue.squeue }

(* default size of one queue *)
let shm_size = ref (1024 * 1024 * 1024)

let set_shm_size new_size =
  shm_size := new_size

let get_shm_size () =
  !shm_size

(* queue for parallel processing *)
module Pqueue = struct

  let create name =
    let mem_pool_id = Netmcore_mempool.create_mempool !shm_size in
    let queue = Netmcore_queue.create mem_pool_id () in
    { id = mem_pool_id;
      name = name;
      q = queue }

  let destroy q =
    Netmcore_mempool.unlink_mempool q.id

  let rec push queue (x: 'a list): unit =
    let could_push =
      try (Netmcore_queue.push x queue.q; true) (* push elt *)
      with Netmcore_mempool.Out_of_pool_memory -> false in
    if not could_push then
      begin (* queue is full *)
        let current_size = Netmcore_queue.length queue.q in
        if !debug then
          Pr.eprintf "warn: Pqueue.push: %s: full: %d messages\n%!"
            queue.name current_size;
        (* apparently, trying to push to a full queue monopolizes the semaphore
           and prevents clients from popping.
           So, we wait for the size to significantly decrease before trying again *)
        Unix.sleepf 0.001;
        let low_water_mark = (current_size * 10) / 100 in
        while Netmcore_queue.length queue.q >= low_water_mark do
          Unix.sleepf 0.001
        done;
        push queue x (* try to push again *)
      end

  let rec process_one queue (f: 'a list -> unit): unit =
    let could_pop =
      (* Netmcore_queue.pop_p avoids data copy out of the shared heap *)
      try (Netmcore_queue.pop_p queue.q f; true)
      with Netmcore_queue.Empty -> false in
    if not could_pop then
      begin
        if !debug then
          Pr.eprintf "warn: Pqueue.process_one: empty: %s\n%!"
            queue.name;
        Unix.sleepf 0.001;
        while Netmcore_queue.is_empty queue.q do
          Unix.sleepf 0.001
        done;
        process_one queue f
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
      for _ = 1 to csize do
        let x = demux () in
        to_send := x :: !to_send
      done;
      Pqueue.push queue !to_send;
      to_send := []
    done
  with End_of_input ->
    begin
      if !to_send <> [] then Pqueue.push queue !to_send;
      (* tell workers to stop *)
      (* printf "feeder %d: telling workers to stop\n%!" pid; *)
      for _ = 1 to nprocs do
        Pqueue.push queue []
      done
    end

(* worker process loop *)
let go_to_work jobs_queue work results_queue =
  (* let pid = Unix.getpid () in *)
  (* printf "worker %d: started\n%!" pid; *)
  let finished = ref false in
  while not !finished do
    Pqueue.process_one jobs_queue (function
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
      (* parallel version *)
      (* let pid = Unix.getpid () in *)
      (* printf "father %d: started\n%!" pid; *)
      (* create queues *)
      let jobs_queue = Pqueue.create "jobs_in" in
      let results_queue = Pqueue.create "results_out" in
      (* start feeder *)
      (* printf "father %d: starting feeder\n%!" pid; *)
      Gc.compact (); (* like parmap: reclaim memory prior to forking *)
      fork_out (fun () -> feed_them_all csize nprocs demux jobs_queue);
      (* start workers *)
      for _ = 1 to nprocs do
        (* printf "father %d: starting a worker\n%!" pid; *)
        fork_out (fun () -> go_to_work jobs_queue work results_queue)
      done;
      (* collect results *)
      let nb_finished = ref 0 in
      while !nb_finished < nprocs do
        Pqueue.process_one results_queue (fun msg ->
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
