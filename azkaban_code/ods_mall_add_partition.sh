#!/bin/bash
# 给ODS层的表添加分区，这个脚本每天执行一次
# 每天凌晨添加昨天的分区，添加完分区之后再执行后面的计算脚本

# 默认获取昨天的日期，也支持传参指定一个日期
if [ "z$1" = "z" ]
then
    dt=`date +%Y%m%d --date="1 days ago"`
else
    dt=$1
fi

sh /data/soft/warehouse_job/add_partition.sh ods_mall.ods_user_order ${dt} ${dt}