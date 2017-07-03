
exception End_of_input

val run:
  ?csize:int ->
  nprocs:int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit
