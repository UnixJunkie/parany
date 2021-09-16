#!/bin/bash

#set -x # DEBUG

NPROCS=`getconf _NPROCESSORS_ONLN`

# parallel with csize > 1
diff <(seq 0 99999) \
     <(_build/default/src/test.exe $NPROCS 800 0 2>/dev/null | sort -n)

# parallel with csize = 1
diff <(seq 0 99999) \
     <(_build/default/src/test.exe $NPROCS 1 0 2>/dev/null | sort -n)

# parallel with csize = 1 and preserve
# DO NOT IGNORE STDERR because testing init/finalize with preserve
diff <(seq 0 99999) \
     <(_build/default/src/test.exe $NPROCS 1 1)

# sequential
diff <(seq 0 99999) \
     <(_build/default/src/test.exe 1 1 0 2>/dev/null | sort -n)
