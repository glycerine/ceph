#!/bin/bash

set -e

media=$1
empty_methods="lua_empty native_empty"
input_methods="lua_write_input native_write_input"
input_size=4096
poolprefix="rep"
skip=30
nops=10000
dur=10
datadir="results"

#separate obj?
for rep in `seq 1 3`; do
  pool="${poolprefix}${rep}"
  outfile_base="${media}_${rep}x"
  common="--pool $pool --lua_cost false --cls lua --skip $skip --dur $dur --obj obj"
  for method in $empty_methods; do
    outfile="${outfile_base}_${method}.dat"
    echo $outfile
    time ./method_call_overhead $common --method $method --isize 0 | tee $datadir/$outfile
  done
  for method in $input_methods; do
    outfile="${outfile_base}_${method}_${input_size}.dat"
    echo $outfile
    time ./method_call_overhead $common --method $method --isize $input_size | tee $datadir/$outfile
  done
done
