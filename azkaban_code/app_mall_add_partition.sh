#!/bin/bash
# 需求：电商GMV
# 每天凌晨执行一次

# 默认获取昨天的日期，也支持传参指定一个日期
if [ "z$1" = "z" ]
then
    dt=`date +%Y%m%d --date="1 days ago"`
else
    dt=$1
fi

hive -e "
insert overwrite table app_mall.app_gmv partition(dt='${dt}')  select
sum(order_money) as gmv
from dwd_mall.dwd_user_order
where dt = '${dt}';
"