#!/bin/sh
for i in `seq 1 8`; do
  sh createsql.sh db$i &
done;

