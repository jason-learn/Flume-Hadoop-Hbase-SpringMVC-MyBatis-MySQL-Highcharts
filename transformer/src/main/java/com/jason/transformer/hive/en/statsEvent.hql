-- 1. 在hive中创建hbase的event_logs对应表
CREATE EXTERNAL TABLE event_logs(row string, pl string, en string, s_time bigint, p_url string, u_ud string, u_sd string, ca string, ac string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties('hbase.columns.mapping'=':key,info:pl,info:en,info:s_time,info:p_url,info:u_ud,info:u_sd,info:ca,info:ac')
tblproperties('hbase.table.name'='event_logs');

-- 2. 创建mysql在hive中的对应表
CREATE TABLE `stats_event` (`platform_dimension_id` bigint ,`data_dimension_id` bigint , `event_dimension_id` bigint , `times` bigint , `created` string);

-- 3. 编写UDF(eventdimension)<需要注意，要删除DimensionConvertClient类中所有FileSystem关闭的操作>
-- 4. 上传transformer-0.0.1.jar到hdfs的/jason/transformer文件夹中
-- 5. 创建hive的function
create function event_convert as 'com.jason.transformer.hive.EventDimensionUDF' using jar 'hdfs://hh:8020/jason/transformer/transformer-0.0.1.jar';  

-- 6. hql编写<注意：时间为外部给定>
with tmp as 
(
select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,ca,ac
from event_logs
where en='e_e' and pl is not null and s_time >= unix_timestamp('2015-12-13','yyyy-MM-dd')*1000 and s_time < unix_timestamp('2015-12-14','yyyy-MM-dd')*1000 
)
from (
select pl as pl,date,ca as ca,ac as ac,count(1) as times from tmp group by pl,date,ca,ac union all
select 'all' as pl,date,ca as ca,ac as ac,count(1) as times from tmp group by date,ca,ac union all
select pl as pl,date,ca as ca,'all' as ac,count(1) as times from tmp group by pl,date,ca union all
select 'all' as pl,date,ca as ca,'all' as ac,count(1) as times from tmp group by date,ca union all
select pl as pl,date,'all' as ca,'all' as ac,count(1) as times from tmp group by pl,date union all
select 'all' as pl,date,'all' as ca,'all' as ac,count(1) as times from tmp group by date
) as tmp2
insert overwrite table stats_event select platform_convert(pl),date_convert(date),event_convert(ca,ac),sum(times),date group by pl,date,ca,ac

-- 7. sqoop脚步编写
sqoop export --connect jdbc:mysql://localhost:3306/report --username hive --password hive --table stats_event --export-dir /hive/bigdater.db/stats_event/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,event_dimension_id

-- 8. shell脚步编写
