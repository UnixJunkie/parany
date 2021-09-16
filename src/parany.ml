
module Dom = Domainslib
module Chan = Dom.Chan

exception End_of_input

type 'a work = Work of 'a
             | No_work

type 'b res = Res of 'b
            | No_result

let worker_loop jobs results work =
  let rec loop () =
    match Chan.recv jobs with
    | No_work ->
      (* signal muxer thread *)
      Chan.send results No_result
    | Work x ->
      let y = work x in
      Chan.send results (Res y);
      loop () in
  loop ()

let demuxer_loop nprocs jobs demux =
  let rec loop () =
    try
      let job = demux () in
      Chan.send jobs (Work job);
      loop ()
    with End_of_input ->
      begin
        for _i = 1 to nprocs do
          (* signal each worker *)
          Chan.send jobs No_work
        done
      end in
  loop ()

let muxer_loop nprocs results mux =
  let finished = ref 0 in
  let rec loop () =
    match Chan.recv results with
    | No_result -> (* one worker finished *)
      begin
        incr finished;
        if !finished = nprocs then
          ()
        else
          loop ()
      end
    | Res x ->
      begin
        mux x;
        loop ()
      end
  in
  loop ()

let run ?(preserve = false) ?(csize = 1) (nprocs: int) ~demux ~work ~mux =
  if preserve then
    failwith "Parany.run: preserve not supported";
  if csize > 1 then
    failwith "Parany.run: csize not supported";
  if nprocs <= 1 then
    (* no overhead sequential version *)
    try
      while true do
        mux (work (demux ()))
      done
    with End_of_input -> ()
  else
    begin
      (* nprocs workers + demuxer thread + muxer thread *)
      let jobs = Chan.make_bounded (50 * nprocs) in
      let results = Chan.make_bounded (50 * nprocs) in
      (* launch the workers *)
      let workers =
        Array.init nprocs (fun _rank ->
            Domain.spawn (fun _ ->
                worker_loop jobs results work
              )
          ) in
      (* launch the demuxer *)
      let demuxer = Domain.spawn (fun _ ->
          demuxer_loop nprocs jobs demux
        ) in
      (* use the current thread to collect results (muxer) *)
      muxer_loop nprocs results mux;
      (* wait all threads *)
      Domain.join demuxer;
      Array.iter Domain.join workers
    end
