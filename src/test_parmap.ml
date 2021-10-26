
(* test preserve in Parany.Parmap *)
let main () =
  let input = [1;2;3;4;5;6;7;8] in
  let id x =
    x in
  let idi _i x =
    x in
  let ncores = 3 in
  let output1 = Parany.Parmap.parmap ~preserve:true ncores id input in
  assert(input = output1);
  let output2 = Parany.Parmap.parmap ~preserve:false ncores id input in
  assert(input = List.sort compare output2);
  let output3 = Parany.Parmap.parmapi ~preserve:true ncores idi input in
  assert(input = output3);
  let output4 = Parany.Parmap.parmapi ~preserve:false ncores idi input in
  assert(input = List.sort compare output4)

let () = main ()
