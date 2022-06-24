#!/bin/bash
# 给外部分区表添加分区
# 接收3个参数
#1：表名
#2：分区字段dt的值：格式20260101
#3：分区路径(相对路径或者绝对路径都可以)

if [ $# != 3 ]
then
    echo "参数异常：add_partition.sh <tabkle_name> <dt> <path>"
    exit 100
fi

table_name=$1
dt=$2
path=$3

hive -e "
alter table ${table_name} add if not exists partition(dt='${dt}') location '${path}';
"