#!/bin/sh
for i in `seq 1 10000`; do
        mysql -u root -h 127.0.0.1 -P 4000 -e "select count(*) from information_schema.tables;";
        sleep 0.3 
done;
