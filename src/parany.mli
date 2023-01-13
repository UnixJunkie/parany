
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
    Functions [f] and [g] are run by different threads.
    Function [g] is run in parallel by several threads
    (16 in this example).
    Only function [mux] is run by the same thread that called [Parany.run].
    [~preserve] is an optional parameter which defaults to false.
    If set to true, results will be accumulated by [h] in the same
    order that function [f] emitted them. However, for parallel performance
    reasons, the jobs are still potentially computed by [g] out of order. *)
val run:
  ?preserve:bool ->
  ?csize:int ->
  int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit

(** INSIDE OF ITS WORK FUNCTION, a parallel worker can call [get_rank()]
    to know its rank. The first spawned worker thread has rank 0.
    The second one has rank 1, etc.
    With N parallel workers, ranks are in [0..N-1]. *)
val get_rank: unit -> int

module DLS: sig

  (** a domain/thread private store *)
  type 'a store

  (** [create (fun () -> zero_elt)] create an empty store.
      For typing reasons, you need to pass a function which
      returns an example element if applied. *)
  val create: (unit -> 'a) -> 'a store

  (** [set store x] put [x] in the [store] for the current domain,
      so that it can be retrieved later on. *)
  val set: 'a store -> 'a -> unit

  (** [get store] retrieve something from the [store] for the current domain.
      @raise Not_found if the current domain/thread has not called [set]
      previously. *)
  val get: 'a store -> 'a

end

(** Wrapper module for near-compatibility with Parmap *)
module Parmap: sig

  (** Parallel List.map *)
  val parmap: ?preserve:bool -> ?csize:int -> int ->
    ('a -> 'b) -> 'a list -> 'b list

  (** Parallel List.mapi *)
  val parmapi: ?preserve:bool -> ?csize:int -> int ->
    (int -> 'a -> 'b) -> 'a list -> 'b list

  (** Parallel List.iter *)
  val pariter: ?preserve:bool -> ?csize:int -> int ->
    ('a -> unit) -> 'a list -> unit

  (** Parallel List.fold *)
  val parfold: ?preserve:bool -> ?csize:int -> int ->
    ('a -> 'b) -> ('acc -> 'b -> 'acc) -> 'acc -> 'a list -> 'acc

  (** Parallel Array.map; array input order is always preserved. *)
  val array_parmap: int -> ('a -> 'b) -> 'b -> 'a array -> 'b array

end
