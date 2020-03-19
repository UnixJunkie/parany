
module A = Array

open Printf

let unmarshal_from_file fn =
  let fd = Unix.(openfile fn [O_RDONLY] 0) in
  let a =
    Bigarray.array1_of_genarray
      (Unix.map_file fd Bigarray.char Bigarray.c_layout true [|-1|]) in
  let res = Bytearray.unmarshal a 0 in
  Unix.close fd;
  Sys.remove fn;
  res

let marshal_to_file v =
  let tmp_fn = Filename.temp_file "parany_" "" in
  let fd = Unix.(openfile tmp_fn [O_WRONLY; O_CREAT; O_EXCL] 0o600) in
  let s = Marshal.to_string v [Marshal.No_sharing] in
  ignore(Bytearray.mmap_of_string fd s);
  Unix.close fd

let main () =
  let fd_in, fd_out = Unix.(socketpair PF_UNIX SOCK_DGRAM 0) in
  let message_fn = "/tmp/parany_shm_file" in
  let _message = A.init 10 (fun i -> i) in
  let sent = Sendmsg.send fd_in (Bytes.unsafe_of_string message_fn) 0 (String.length message_fn) in
  printf "sent: %d\n" sent;
  Unix.close fd_in;
  let buff = Bytes.create 80 in
  let received, _none = Sendmsg.recv fd_out buff 0 80 in
  printf "received: %d\nmsg: %s\n" received (Bytes.unsafe_to_string buff);
  Unix.close fd_out

let () = main ()
