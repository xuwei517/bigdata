#!/bin/bash
sqoop export \
--connect jdbc:mysql://192.168.182.1:3306/data?serverTimezone=UTC \
--username root \
--password admin \
--table user2 \
--export-dir /out2 \
--input-fields-terminated-by '\t'