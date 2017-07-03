
open Netcamlbox
open Printf

module Inbox = Mbox.Readable
module Outbox = Mbox.Writable

exception End_of_input

(* feeder process main loop *)
let feed_them_all
    (csize: int)
    (demux: unit -> 'a)
    (outboxes: ('a Mbox.message Netcamlbox.camlbox_sender) list): unit =
  (* let pid = Unix.getpid () in *)
  (* printf "feeder %d: started\n%!" pid; *)
  try
    while true do
      List.iter (fun outbox ->
          let can_accept = Outbox.count_free_spots outbox in
          for i = 1 to can_accept do
            Outbox.write outbox (Mbox.Msg (demux ()));
            (* printf "feeder %d: sent one\n%!" pid *)
          done
        ) outboxes
    done
  with End_of_input ->
    (* tell workers to stop *)
    ((* printf "feeder %d: telling workers to stop\n%!" pid; *)
      List.iter Outbox.end_of_input outboxes)

(* worker process loop *)
let go_to_work
    (inbox: 'a Mbox.message camlbox)
    (f: 'a -> 'b)
    (outbox: 'b Mbox.message camlbox_sender): unit =
  (* let pid = Unix.getpid () in *)
  (* printf "worker %d: started\n%!" pid; *)
  let nb_finished = ref 0 in
  while !nb_finished < 1 do
    let finished =
      Inbox.process_many inbox
        (fun x ->
           (* printf "worker %d: did one\n%!" pid; *)
           Outbox.write outbox (Mbox.Msg (f x))) in
    nb_finished := finished + !nb_finished
  done;
  (* tell collector to stop *)
  (* printf "worker %d: I'm done\n%!" pid; *)
  Outbox.end_of_input outbox

let add_to lref x =
  lref := x :: !lref

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> let () = f () in exit 0
  | _pid -> ()

let run ?csize:(csize = 1) ~nprocs ~demux ~work ~mux =
  if nprocs = 1 then
    (* sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    begin
      let pid = Unix.getpid () in
      (* printf "father %d: started\n%!" pid; *)
      (* parallel version *)
      assert(nprocs > 1);
      (* create all boxes *)
      let feeder_boxes = ref [] in
      let worker_boxes = ref [] in
      let out_mbox_name = sprintf "mbox_out_%d" pid in
      (* FBR: 1024 is max_msg_size_out *)
      (* the workers' outbox is two times bigger than necessary so that
         workers should not wait when writing results out *)
      let collector_inbox = Inbox.create out_mbox_name (2 * csize * nprocs) 1024 in
      let workers_outbox = Outbox.create out_mbox_name in
      for i = 1 to nprocs do
        let in_mbox_name = sprintf "mbox_in_%d_%d" pid i in
        (* FBR: 1024 is max_msg_size_in *)
        let worker_input = Inbox.create in_mbox_name csize 1024 in
        let feeder_output = Outbox.create in_mbox_name in
        add_to feeder_boxes feeder_output;
        add_to worker_boxes (worker_input, workers_outbox);
      done;
      (* start feeder *)
      (* printf "father %d: starting feeder\n%!" pid; *)
      fork_out (fun () -> feed_them_all csize demux !feeder_boxes);
      (* start workers *)
      List.iter (fun (inbox, outbox) ->
          (* printf "father %d: starting a worker\n%!" pid; *)
          fork_out (fun () -> go_to_work inbox work outbox)
        ) !worker_boxes;
      (* collect results *)
      let nb_finished = ref 0 in
      while !nb_finished < nprocs do
        let finished =
          Inbox.process_many collector_inbox mux
            (* (fun x -> *)
            (*    printf "father %d: collecting one\n%!" pid; *)
            (*    mux x) *)
        in
        nb_finished := finished + !nb_finished
      done;
      (* delete readable boxes (writable boxes don't need to be) *)
      List.iter (fun (input, _) -> Inbox.destroy input) !worker_boxes;
      Inbox.destroy collector_inbox
    end
