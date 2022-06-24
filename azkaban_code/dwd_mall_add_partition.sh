#!/bin/bash
# 基于ODS层的表进行清洗，将清洗之后的数据添加到DWD层对应表的对应分区中
# 每天凌晨执行一次

# 默认获取昨天的日期，也支持传参指定一个日期
if [ "z$1" = "z" ]
then
    dt=`date +%Y%m%d --date="1 days ago"`
else
    dt=$1
fi

hive -e "
insert overwrite table dwd_mall.dwd_user_order partition(dt='${dt}')  select
   order_id,
   order_date,
   user_id,
   order_money,
   order_type,
   order_status,
   pay_id,
   update_time
from ods_mall.ods_user_order
where dt = '${dt}' and order_id is not null;
"