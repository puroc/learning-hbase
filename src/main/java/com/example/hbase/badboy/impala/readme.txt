[hbase建表]
create 'metrics_hbase_test','lz'

[hbase插入数据]
put 'metrics_hbase_test','row1','lz:id','device0'
put 'metrics_hbase_test','row1','lz:reading',123
put 'metrics_hbase_test','row1','lz:realvalue',123
put 'metrics_hbase_test','row1','lz:time','2018-06-28 18:00:00'

[查询hbase数据]
scan 'metrics_hbase_test',{LIMIT=>1}

[hive建表]
CREATE EXTERNAL TABLE metrics_hbase( ID STRING, READING INT, REALVALUE INT, TIME STRING)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
 WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, lz:READING, lz:REALVALUE, lz:TIME")
 TBLPROPERTIES("hbase.table.name" = "metrics_hbase_test");

CREATE EXTERNAL TABLE metrics_hbase( id STRING, reading INT, realvalue INT, time STRING)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
 WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, lz:reading, lz:realvalue, lz:time")
 TBLPROPERTIES("hbase.table.name" = "metrics_hbase_test");

[impala从hive同步表]
INVALIDATE METADATA;

[查询语句]
explain select
  T_3C75F1.`deviceid`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`),
  sum(T_3C75F1.`reading`),
  count(1)
from (select device_parquet.deviceid,reading,to_timestamp(time,'yyyy-MM-dd HH:mm:ss') as time from KUDU_WATER_HISTORY,device_parquet where KUDU_WATER_HISTORY.device=device_parquet.deviceid) as `T_3C75F1`
group by
  T_3C75F1.`deviceid`,
  year(T_3C75F1.`time`),
  month(T_3C75F1.`time`);

select * from metrics_hbase limit 10;