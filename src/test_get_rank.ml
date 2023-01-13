
module A = Array

let main () =
  let n = 1_000_000 in
  let nprocs = 24 in
  let a = A.make n (-1) in
  let working = ref 0 in
  let demux () =
    if !working = nprocs then
      raise Parany.End_of_input
    else
      let () = Printf.printf "sending %d\n%!" !working in
      incr working
  in
  let work () =
    let my_id = (Domain.self() :> int) in
    let my_rank = Parany.get_rank () in
    let msg = Printf.sprintf "my_id: %d my_rank: %d\n" my_id my_rank in
    Printf.printf "%s%!" msg;
    let i = ref 0 in
    while !i < n do
      let j = !i + my_rank in
      (if j < n then
         a.(j) <- (j)
      );
      i := !i + nprocs
    done
  in
  let finished = ref 0 in
  let mux () =
    incr finished
  in
  Parany.run nprocs ~demux ~work ~mux;
  (* check array content is what we expect *)
  A.iteri (fun i x ->
      (* Printf.printf "%d %d\n" i x; *)
      assert(i = x)
    ) a;
  Printf.printf "first pass OK\n%!";
  (* reset things *)
  for i = 0 to n - 1 do
    a.(i) <- -1
  done;
  working := 0;
  finished := 0;
  (* try again *)
  Parany.run nprocs ~demux ~work ~mux;
  A.iteri (fun i x ->
      (* Printf.printf "%d %d\n" i x; *)
      assert(i = x)
    ) a;
  Printf.printf "second pass OK\n%!"

let () = main ()
