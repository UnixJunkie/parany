
(** The [demux] function must throw [Parany.End_of_input] once it is done. *)
exception End_of_input

(** [enable_core_pinning ()] turn ON pinning worker processes to
    distinct cores; default is OFF. Must be called before [run].
    Worker processes are pinned to cores [0..(nprocs-1)].
    Don't turn this ON on a shared computer with several users, or if you
    are running several parany-enabled jobs on the same computer. *)
val enable_core_pinning: unit -> unit

(** [disable_core_pinning ()] turn OFF core pinning. cf. [enable_core_pinning]. *)
val disable_core_pinning: unit -> unit

(** [run ~csize:10 ~nprocs:16 ~demux:f ~work:g ~mux:h] will run
    in parallel on 16 cores the [g] function.
    Inputs to function [g] are produced by function [f]
    and grouped by 10 (the chunk size [csize]).
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

(** Wrapper module for near-compatibility with Parmap *)
module Parmap: sig

  (** Parallel List.map *)
  val parmap: ncores:int -> ?csize:int ->
    ('a -> 'b) -> 'a list -> 'b list

  (** Parallel List.iter *)
  val pariter: ncores:int -> ?csize:int ->
    ('a -> unit) -> 'a list -> unit

  (** Parallel List.fold *)
  val parfold: ncores:int -> ?csize:int ->
    ('a -> 'b) -> ('c -> 'b -> 'c) -> 'c -> 'a list -> 'c
end
