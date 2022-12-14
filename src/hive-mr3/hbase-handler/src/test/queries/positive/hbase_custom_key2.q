CREATE EXTERNAL TABLE hbase_ck_4(key struct<col1:string,col2:string,col3:string>, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.table.name" = "hbase_custom2",
    "hbase.columns.mapping" = ":key,cf:string",
    "hbase.composite.key.factory"="org.apache.hadoop.hive.hbase.SampleHBaseKeyFactory2")
TBLPROPERTIES ("external.table.purge" = "true");

from src tablesample (5 rows)
insert into table hbase_ck_4 select
struct(
  cast(key as string),
  cast(cast(key + 1000 as int) as string),
  cast(cast(key + 2000 as int) as string)),
value;

set hive.fetch.task.conversion=more;

-- 165,238,27,311,86
select * from hbase_ck_4;

-- 238
explain
select * from hbase_ck_4 where key.col1 = '238' AND key.col2 = '1238';
select * from hbase_ck_4 where key.col1 = '238' AND key.col2 = '1238';

-- 165,238
explain
select * from hbase_ck_4 where key.col1 >= '165' AND key.col1 < '27';
select * from hbase_ck_4 where key.col1 >= '165' AND key.col1 < '27';

-- 238,311
explain
select * from hbase_ck_4 where key.col1 > '100' AND key.col2 >= '1238';
select * from hbase_ck_4 where key.col1 > '100' AND key.col2 >= '1238';

