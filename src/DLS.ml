
module IntMap = Map.Make(Int)
module Sem = Semaphore.Binary

let write_lock = Sem.make true

(* apparently, null_f is required to avoid the weak type variable problem *)
type 'a store = { null_f: (unit -> 'a);
                  values: ('a IntMap.t) ref }

let create (null_f: unit -> 'a): 'a store =
  { null_f; values = ref IntMap.empty }

let set (s: 'a store) (v: 'a): unit =
  let my_id = (Domain.self () :> int) in
  Sem.acquire write_lock; (* <critical_section> *)
  s.values := IntMap.add my_id v !(s.values);
  Sem.release write_lock (* </critical_section> *)

let get s =
  IntMap.find (Domain.self () :> int) !(s.values)
