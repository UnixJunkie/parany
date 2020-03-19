
open Printf

let main () =
  let fd_in, fd_out = Unix.(socketpair PF_UNIX SOCK_DGRAM 0) in
  let message = "Hello boy!" in
  let sent = Sendmsg.send fd_in (Bytes.unsafe_of_string message) 0 (String.length message) in
  printf "sent: %d\n" sent;
  Unix.close fd_in;
  let buff = Bytes.create 80 in
  let received, _none = Sendmsg.recv fd_out buff 0 80 in
  printf "received: %d\nmsg: %s\n" received (Bytes.unsafe_to_string buff);
  Unix.close fd_out

let () = main ()
