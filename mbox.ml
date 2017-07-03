
open Netcamlbox
open Printf

type 'a message =
  | Msg of 'a
  | Last_message of int (* Tell receiver nothing more will come.
                           The int param is just here to make sure
                           this value is boxed (so that netcamlbox
                           can (de)serialize it properly). *)

(* An mbox to read from. This module really creates and destroys the mbox. *)
module Readable = struct

  let create (name: string) (max_nb_msg: int) (max_msg_size: int)
    : 'a message camlbox =
    try create_camlbox name max_nb_msg max_msg_size
    with Unix.Unix_error(Unix.EEXIST, "shm_open", _) ->
      (eprintf "Mbox.Readable.create: overwriting %s\n" name;
       unlink_camlbox name;
       create_camlbox name max_nb_msg max_msg_size)

  let destroy (box: 'a message camlbox): unit =
    unlink_camlbox (camlbox_addr box)

  (* process as many messages as possible;
     return the number of Last_message that were read in the box
     WARNING: will block until there is something to process *)
  let process_many (box: 'a message camlbox) (f: 'a -> unit): int =
    let msg_ids = camlbox_wait box in
    let last_msg_count = ref 0 in
    List.iter (fun msg_id ->
        (* data copy because of deserialization is avoided here
           thanks to netcamlbox *)
        let msg = camlbox_get box msg_id in
        begin match msg with
          | Last_message _ -> incr last_msg_count
          | Msg x -> f x
        end;
        (* free spot ASAP *)
        camlbox_delete box msg_id
      ) msg_ids;
    !last_msg_count

end

(* An mbox to write to. It is only retrieved, not created by this module. *)
module Writable = struct

  let create (name: string): 'a message camlbox_sender =
    camlbox_sender name

  (* WARNING: may block until enough space in dst box;
     serialization is done here *)
  let write (box: 'a message camlbox_sender) (msg: 'a message): unit =
    camlbox_send box msg

  (* tell the reader no more messages will come *)
  let end_of_input (box: 'a message camlbox_sender): unit =
    camlbox_send box (Last_message 1)

  (* how many messages can we send without blocking *)
  let count_free_spots (box: 'a message camlbox_sender): int =
    (camlbox_scapacity box) - (camlbox_smessages box)

end
