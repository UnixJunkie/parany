
exception End_of_input

val run:
  csize:int ->
  nprocs:int ->
  demux:(unit -> 'a) ->
  f:('a -> 'b) ->
  mux:('b -> unit) -> unit
