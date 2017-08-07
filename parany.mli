
(** The [demux] function must throw [Parany.End_of_input] once it is done. *)
exception End_of_input

(** [run ~csize:10 ~nprocs:16 ~demux:f ~work:g ~mux:h] will run
    in parallel on 16 cores the [g] function.
    Inputs to function [g] are produced by function [f]
    and grouped by 10 (the chunk size).
    The demux function [f] must throw [Parany.End_of_input]
    once it is done.
    Outputs of function [g] are consumed by function [h].
    Functions [f] and [g] are run by different unix processes.
    Function [g] is run by [nprocs] processes at the same time.
    Only function [mux] is run by the same process that called
    [Parany.run].
    A [csize] of one should be the default.
    The optimal [csize] depends on your computer, the functions
    you are using and the granularity of your computation.
    Elements which are fast to process may benefit from
    a [csize] greater than one. *)
val run:
  csize:int ->
  nprocs:int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit
