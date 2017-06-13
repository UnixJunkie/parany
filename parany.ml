
exception End_of_input

let run (nprocs: int) (demux: unit -> 'a) (f: 'a -> 'b) (mux: 'b -> unit)
  : unit =
  if nprocs = 1 then (* sequential version *)
    try
      while true do
        mux (f (demux ()))
      done
    with End_of_input -> ()
  else (* parallel version *)
    failwith "not implemented yet"
