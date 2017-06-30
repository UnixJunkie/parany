
open Printf

let n = 1_000_0000
let inputs = Array.init n (fun i -> i)
let counter = ref 0

let demux () =
  if !counter = n then
    raise Parany.End_of_input
  else
    let res = inputs.(!counter) in
    incr counter;
    res

let work a =
  a

let res_counter = ref 0
let results = Array.make n 0

let mux x =
  (* Log.info "Test.mux: %d" x; *)
  results.(!res_counter) <- x;
  incr res_counter

let main () =
  let argc = Array.length Sys.argv in
  if argc <> 2 then
    (eprintf "usage: %s <int>\n" Sys.argv.(0);
     exit 1);
  let nprocs = int_of_string Sys.argv.(1) in
  Parany.run ~csize:1 ~nprocs ~demux ~f:work ~mux;
  for i = 0 to n - 1 do
    Printf.printf "%d\n" results.(i)
  done

let () = main ()
