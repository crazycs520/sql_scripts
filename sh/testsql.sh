#!/bin/sh
for i in `seq 1 10000`; do
        mysql -u root -h 127.0.0.1 -P 4000 -e "select count(*) from information_schema.tables;";
        if [ $? != 0 ]; then
                ps aux|grep createsql| grep -v grep |awk '{print $2}'|xargs kill -9
                exit
        fi
        sleep 1 
done;
