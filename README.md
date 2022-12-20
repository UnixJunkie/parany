# parany
Parallelize any kind of computation

Generalized map reduce for parallel computers (not distributed computing).

Can process a very large file in parallel on a multi-core computer;
provided there is a way to cut your file into independent blocks (the "demux"
function).

Can process in parallel an infinite stream of elements
(if you don't care about their processing order).
In practice, if the stream is really infinite but you want to collect
results in order, some integer counter will eventually overflow and
the behavior is unspecified and unknown after this unfortunate event.

The processing function is called "work".
The function gathering the results is called "mux".
