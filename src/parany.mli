
(** The [demux] function must throw [Parany.End_of_input] once it is done. *)
exception End_of_input

(** [run ~csize:10 ~nprocs:16 ~demux:f ~work:g ~mux:h] will run
    in parallel on 16 cores the [g] function.
    Inputs to function [g] are produced by function [f]
    and grouped by 10 (the chunk size [csize]).
    If not provided, [csize] defaults to one.
    The performance-optimal [csize] depends on your computer, the functions
    you are using and the granularity of your computation.
    Elements which are fast to process may benefit from
    a [csize] greater than one.
    The demux function [f] must throw [Parany.End_of_input]
    once it is done.
    Outputs of function [g] are consumed by function [h].
    Functions [f] and [g] are run by different unix processes.
    Function [g] is run by [nprocs] processes at the same time.
    Only function [mux] is run by the same process that called
    [Parany.run].
    ~core_pin is an optional parameter which defaults to false.
    Core pinning can improve performance but should not be used
    on computers with many users or running several parany computations
    at the same time. *)
val run:
  ?core_pin:bool ->
  ?csize:int ->
  nprocs:int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit

(** Wrapper module for near-compatibility with Parmap *)
module Parmap: sig

  (** Parallel List.map *)
  val parmap: ?core_pin:bool -> ncores:int -> ?csize:int ->
    ('a -> 'b) -> 'a list -> 'b list

  (** Parallel List.iter *)
  val pariter: ?core_pin:bool -> ncores:int -> ?csize:int ->
    ('a -> unit) -> 'a list -> unit

  (** Parallel List.fold *)
  val parfold: ?core_pin:bool -> ncores:int -> ?csize:int ->
    ('a -> 'b) -> ('c -> 'b -> 'c) -> 'c -> 'a list -> 'c
end
