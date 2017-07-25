
(* the demux function must throw End_of_input once it's done *)
exception End_of_input

val run: nprocs:int ->
  demux:(unit -> 'a) -> work:('a -> 'b) -> mux:('b -> unit) -> unit
