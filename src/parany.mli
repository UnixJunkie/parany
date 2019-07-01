
(** The [demux] function must throw [Parany.End_of_input] once it is done. *)
exception End_of_input

(** [set_shm_size nb_bytes] set the memory size (in bytes) used by each shared
    memory region. There are two such regions created during [run].
    The default size is 1GB. *)
val set_shm_size: int -> unit

(** [get_shm_size ()] return the current size (in bytes) of one
    shared memory region. There are two such regions created during [run]. *)
val get_shm_size: unit -> int

(** Call [set_copy_on_work ()] to turn ON data copy out of the shared memory
    prior to each call to the [work] function. Default: OFF. *)
val set_copy_on_work: unit -> unit

(** Call [set_copy_on_mux ()] to turn ON data copy out of the shared memory
    prior to each call to the [mux] function. Default: OFF. *)
val set_copy_on_mux: unit -> unit

(** [run ~verbose:false ~csize:10 ~nprocs:16 ~demux:f ~work:g ~mux:h] will run
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
    a [csize] greater than one.
    In order to troubleshoot an application, run parany with [verbose]
    set to true. This will print messages on stderr when a queue
    is empty or full. *)
val run:
  verbose:bool ->
  csize:int ->
  nprocs:int ->
  demux:(unit -> 'a) ->
  work:('a -> 'b) ->
  mux:('b -> unit) -> unit
