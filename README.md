# parany
Parallelize any kind of computation

Generalized map reduce for parallel computers (not distributed computing).

Can process a very large file in parallel on a multicore computer;
provided there is a way to cut your file into independent blocks (the "demux"
function).

Can process in parallel an infinite stream of elements.

The processing function is called "work".
The function gathering the results is called "mux".
The number of processors running your computation in parallel is called
"nprocs".
Jobs can be sent in chunks to workers (parameter chunk size named "csize").
Adjust chunk size to tune the parallelization performance of your application.
