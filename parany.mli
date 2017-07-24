
(* the demux function must throw End_of_input once it's done *)
exception End_of_input

val run: nprocs:int -> demux:(unit -> 'a) -> work:('a -> 'b) -> mux:('b -> unit) -> unit

(* Unnamed posix semaphore with a simple interface *)
module Sem : sig
  exception Sem_except of string
  type 'a t
  val create_exn: int -> [> `Unnamed] t
  val destroy_exn: [> `Unnamed] t -> unit
  val post_exn: [> `Unnamed] t -> unit
  val wait_exn: [> `Unnamed] t -> unit
  val try_wait: [> `Unnamed] t -> bool
end
