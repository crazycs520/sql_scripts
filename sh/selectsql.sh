#!/bin/sh
for i in `seq 1 10000`; do
        mysql -u root -h 127.0.0.1 -P 4000 -e "select * from mysql.gc_delete_range;";
        sleep 0.1
done;
