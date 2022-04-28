
open Printf

let n = 100_000
let inputs = Array.init n (fun i -> i)
let counter = ref 0

let demux () =
  if !counter = n then
    raise Parany.End_of_input
  else
    let res = inputs.(!counter) in
    incr counter;
    res

let res_counter = ref 0
let results = Array.make n 0

let mux x =
  results.(!res_counter) <- x;
  incr res_counter

let bool_of_string = function
  | "0" -> false
  | "1" -> true
  | x -> failwith (sprintf "Test.bool_of_string: %s neither 0 nor 1" x)

let rank = ref (-1)
let first_time = ref true

let hello i =
  rank := i;
  eprintf "I am %d\n%!" i

let goodbye () =
  eprintf "Goodbye from %d\n%!" !rank

let work a =
  (* if !first_time then
   *   begin
   *     eprintf "My rank is %d\n%!" (Parany.get_rank ());
   *     first_time := false
   *   end; *)
  (* if !rank = 8 then failwith "Worker 8 on strike" else *)
  a

let main () =
  let argc = Array.length Sys.argv in
  if argc <> 4 then
    (eprintf "usage: %s nprocs csize {0|1}\n" Sys.argv.(0);
     exit 1);
  let nprocs = int_of_string Sys.argv.(1) in
  let csize = int_of_string Sys.argv.(2) in
  let preserve = bool_of_string Sys.argv.(3) in
  (* Parany.set_core_pinning true; *)
  (if preserve then
     (* we test init/finalize only with preserve *)
     Parany.run ~init:hello ~finalize:goodbye ~preserve:true ~csize:csize nprocs ~demux ~work ~mux
   else
     Parany.run ~csize:csize nprocs ~demux ~work ~mux);
  for i = 0 to n - 1 do
    Printf.printf "%d\n" results.(i)
  done

let () = main ()
