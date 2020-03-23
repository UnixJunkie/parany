
open Printf

let unmarshal_from_file fn =
  (* mmap -> O_RDWR *)
  let fd = Unix.(openfile fn [O_RDWR] 0) in
  let a =
    Bigarray.array1_of_genarray
      (Unix.map_file fd Bigarray.char Bigarray.c_layout true [|-1|]) in
  let res = Bytearray.unmarshal a 0 in
  Unix.close fd;
  res

let marshal_to_file fn v =
  (* mmap -> O_RDWR *)
  let fd = Unix.(openfile fn [O_RDWR] 0o600) in
  let s = Marshal.to_string v [Marshal.No_sharing] in
  ignore(Bytearray.mmap_of_string fd s);
  Unix.close fd

let send sock str =
  Sendmsg.send sock (Bytes.unsafe_of_string str) 0 (String.length str)

let receive sock buff =
  let count, none = Sendmsg.recv sock buff 0 (Bytes.length buff) in
  assert(none = None);
  (count, Bytes.sub_string buff 0 count)

let main () =
  let fd_in, fd_out = Unix.(socketpair PF_UNIX SOCK_DGRAM 0) in
  let message_fn = "/tmp/parany_shm_file" in
  let message_out = Array.init 10 (fun i -> i) in
  marshal_to_file message_fn message_out;
  printf "created: %s\n" message_fn;
  let sent = send fd_in message_fn in
  printf "sent: %d\n" sent;
  Unix.close fd_in;
  let buff = Bytes.create 80 in
  let count, message = receive fd_out buff in
  Unix.close fd_out;
  printf "received: %d msg: %s\n" count message;
  let message_in: int array = unmarshal_from_file message in
  printf "decoded:";
  Array.iter (printf " %d") message_in;
  printf "\n"

(* let () = main () *)
