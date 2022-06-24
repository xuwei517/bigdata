#!/bin/bash
# 将MySQL中的数据导入HDFS
if [ $# != 2 ]
then
    echo "参数异常：sqoop_collect_data_util.sh <sql> <hdfs_path>"
    exit 100
fi

# 查询数据SQL
# 例如：select id,name from user where id >1
sql=$1

# 导入HDFS的路径
hdfs_path=$2

sqoop import \
--connect jdbc:mysql://192.168.182.1:3306/mall?serverTimezone=UTC \
--username root \
--password admin \
--target-dir "${hdfs_path}" \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by '\t' \
--query "${sql}"' and $CONDITIONS' \
--null-string '\\N' \
--null-non-string '\\N'