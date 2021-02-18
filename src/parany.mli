
(** The [demux] function must throw [Parany.End_of_input] once it is done. *)
exception End_of_input

(** [run ~csize:10 16 ~demux:f ~work:g ~mux:h] will run
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
    Function [g] is run by several processes at the same time (16
    in this example).
    Only function [mux] is run by the same process that called [Parany.run].
    [~core_pin] is an optional parameter which defaults to false.
    Core pinning can improve performance but should not be used on computers
    with many users or running several parany computations at the same time.
    [~preserve] is an optional parameter which defaults to false.
    If set to true, results will be accumulated by [h] in the same
    order that function [f] emitted them. However, for parallel performance
    reasons, the jobs are still potentially computed by [g] out of order.
    The optional [init] (resp. [finalize]) function is called once
    by each child process just after creation (resp. just before exit).
    [init] and [finalize] both default to doing nothing.
    [init i] takes the child rank [i] as parameter
    (first forked child process has rank 0, next 1, etc.). *)
val run:
  ?init:(int -> unit) ->
  ?finalize:(unit -> unit) ->
  ?preserve:bool ->
  ?core_pin:bool ->
  ?csize:int ->
  int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit

(** Wrapper module for near-compatibility with Parmap *)
module Parmap: sig

  (** Parallel List.map *)
  val parmap: ?init:(int -> unit) -> ?finalize:(unit -> unit) ->
    ?preserve:bool -> ?core_pin:bool -> ?csize:int -> int ->
    ('a -> 'b) -> 'a list -> 'b list

  (** Parallel List.iter *)
  val pariter: ?init:(int -> unit) -> ?finalize:(unit -> unit) ->
    ?preserve:bool -> ?core_pin:bool -> ?csize:int -> int ->
    ('a -> unit) -> 'a list -> unit

  (** Parallel List.fold *)
  val parfold: ?init:(int -> unit) -> ?finalize:(unit -> unit) ->
    ?preserve:bool -> ?core_pin:bool -> ?csize:int -> int ->
    ('a -> 'b) -> ('acc -> 'b -> 'acc) -> 'acc -> 'a list -> 'acc

end
