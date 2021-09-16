
exception End_of_input

let run ?(preserve = false) ?(csize = 1) nprocs ~demux ~work ~mux =
  if preserve then
    failwith "Parany.run: preserve not supported";
  if nprocs <= 1 then
    (* sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    failwith "not implemented yet"
