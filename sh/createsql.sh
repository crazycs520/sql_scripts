#!/bin/sh
DATABASE=$1
mysql -u root -h 127.0.0.1 -P 4000 -e "drop database if exists $DATABASE; create database $DATABASE";
for i in `seq 1 1000000`; do
  mysql -u root -h 127.0.0.1 -P 4000 $DATABASE -e "CREATE TABLE t$i (a int primary key not null auto_increment);";
  if [ $? != 0 ]; then
          exit
  fi

done;
