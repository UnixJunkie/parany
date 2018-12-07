#!/bin/bash

#set -x # DEBUG

NPROCS=`getconf _NPROCESSORS_ONLN`

# parallel with csize > 1
diff <(seq 0 99999) \
     <(_build/default/src/test.exe $NPROCS 800 2>/dev/null | sort -n)

# parallel with csize = 1
diff <(seq 0 99999) \
     <(_build/default/src/test.exe $NPROCS 1 2>/dev/null | sort -n)

# sequential
diff <(seq 0 99999) \
     <(_build/default/src/test.exe 1 1 2>/dev/null | sort -n)
