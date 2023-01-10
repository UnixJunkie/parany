
(* Test the behavior of a global hashtable. Especially, test if the bindings
   are stable over time. *)

let ht = Hashtbl.create 64

let get_domain_id () =
  (Domain.self () :> int)

let get_rank () =
  Hashtbl.find ht (get_domain_id ())

let main () =
  let nprocs = int_of_string Sys.argv.(1) in
  let rng = Random.self_init () in
  (* launch workers *)
  let workers =
    Array.init nprocs (fun my_rank ->
        Domain.spawn (fun () ->
            let my_id = get_domain_id () in
            Hashtbl.add ht my_id my_rank;
            Printf.printf "my_id: %d my_rank: %d\n%!"
              my_id my_rank;
            let my_rng = Random.split rng in
            let some_time = Random.State.float my_rng 1.0 in
            Unix.sleepf some_time;
            Printf.printf "my_id: %d my_rank: %d\n%!"
              my_id (get_rank());
            assert(my_rank = get_rank())
          )
      ) in
  (* wait for all *)
  Array.iter Domain.join workers

let () = main ()
