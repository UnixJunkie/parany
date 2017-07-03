
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
  try
    while true do
      List.iter (fun outbox ->
          let can_accept = Outbox.count_free_spots outbox in
          for i = 1 to can_accept do
            printf "feeder: sending 1 job\n";
            Outbox.write outbox (Mbox.Msg (demux ()));
          done
        ) outboxes
    done
  with End_of_input ->
    (* tell workers to stop *)
    (printf "feeder: telling the guys to stop\n";
     List.iter Outbox.end_of_input outboxes)

(* worker process loop *)
let go_to_work
    (inbox: 'a Mbox.message camlbox)
    (f: 'a -> 'b)
    (outbox: 'b Mbox.message camlbox_sender): unit =
  try
    while true do
      Inbox.process_many inbox
        (fun x ->
           (printf "worker: did 1\n";
            Outbox.write outbox (Mbox.Msg (f x))))
    done
  with Mbox.No_more_work ->
    (* tell collector to stop *)
    (printf "worker: done\n";
     Outbox.end_of_input outbox)

let add_to lref x =
  lref := x :: !lref

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> (f (); exit 0)
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
      (* parallel version *)
      assert(nprocs > 1);
      (* create all boxes *)
      let feeder_boxes = ref [] in
      let collector_boxes = ref [] in
      let worker_boxes = ref [] in
      let pid = Unix.getpid () in
      for i = 1 to nprocs do
        let in_name = sprintf "mbox_in_%d_%d" pid i in
        (* FBR: 1024 is max_msg_size_in *)
        let worker_input = Inbox.create in_name csize 1024 in
        let feeder_output = Outbox.create in_name in
        let out_name = sprintf "mbox_out_%d_%d" pid i in
        (* FBR: 1024 is max_msg_size_out *)
        let collector_input = Inbox.create out_name (2 * csize) 1024 in
        let worker_output = Outbox.create out_name in
        add_to feeder_boxes feeder_output;
        add_to worker_boxes (worker_input, worker_output);
        add_to collector_boxes collector_input
      done;
      (* start feeder *)
      printf "starting feeder\n";
      fork_out (fun () -> feed_them_all csize demux !feeder_boxes);
      printf "feeder started\n";
      (* start workers *)
      printf "starting workers\n";
      List.iter (fun (inbox, outbox) ->
          fork_out (fun () -> go_to_work inbox work outbox)
        ) !worker_boxes;
      printf "workers started\n";
      (* collect results *)
      let nb_workers = ref nprocs in
      while !nb_workers <> 0 do
        List.iter (fun inbox ->
            try Inbox.process_available inbox mux
            with Mbox.No_more_work -> decr nb_workers
          ) !collector_boxes
      done;
      (* delete readable boxes (writable boxes don't need to be) *)
      List.iter (fun (input, _) -> Inbox.destroy input) !worker_boxes;
      List.iter Inbox.destroy !collector_boxes
    end
