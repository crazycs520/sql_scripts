#!/bin/sh
for i in `seq 1 10`; do
        mysql -u root -h 127.0.0.1 -P 4000 test -e "select count(1) from t_slim, t_wide where t_slim.c0>t_wide.c0 and t_slim.c1>t_wide.c1 and t_wide.c0 > $[i * 1000];" &
done;
