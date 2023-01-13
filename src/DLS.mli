
(** a domain/thread private store *)
type 'a store

(** [create (fun () -> zero_elt)] create an empty store.
    For typing reasons, you need to pass a function which
    returns an example element if applied. *)
val create: (unit -> 'a) -> 'a store

(** [set store x] put [x] in the [store] for the current domain
    so that it can be retrieved later. *)
val set: 'a store -> 'a -> unit

(** [get store] retrieve something from the [store] for the current domain.
    @raise Not_found if the current domain/thread has not called [set]
    previously. *)
val get: 'a store -> 'a
