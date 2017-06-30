
exception End_of_input

let fork_out f =
  match Unix.fork () with
  | -1 -> failwith "Parany.fork_out: fork failed"
  | 0 -> (f (); exit 0)
  | _pid -> ()

(* feeder process main loop *)
let feed_them_all () =
  failwith "not implemented yet"

let work input f output =
  failwith "not implemented yet"

(* FBR: csize default value should be 1 *)
let run ~csize ~nprocs ~demux ~f ~mux =
  if nprocs = 1 then (* sequential version *)
    try
      while true do
        mux (f (demux ()))
      done
    with End_of_input -> ()
  else (* parallel version *)
    failwith "not implemented yet"
