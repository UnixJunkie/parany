
module Ht = Hashtbl

(* demux and index items *)
let idemux (demux: unit -> 'a) =
  let demux_count = ref 0 in
  function () ->
    let res = (!demux_count, demux ()) in
    incr demux_count;
    res

(* work ignoring item index *)
let iwork (work: 'a -> 'b) ((i, x): int * 'a): int * 'b =
  (i, work x)

(* mux items in the right order *)
let imux (mux: 'b -> unit) =
  let mux_count = ref 0 in
  (* weak type variable avoidance *)
  let wait_list = Ht.create 11 in
  function (i, res) ->
    if !mux_count = i then
      begin
        (* unpile as much as possible *)
        mux res;
        incr mux_count;
        if Ht.length wait_list > 0 then
          try
            while true do
              let next = Ht.find wait_list !mux_count in
              Ht.remove wait_list !mux_count;
              mux next;
              incr mux_count
            done
          with Not_found -> () (* no more or index hole *)
      end
    else
      (* put somewhere into the pile *)
      Ht.add wait_list i res
