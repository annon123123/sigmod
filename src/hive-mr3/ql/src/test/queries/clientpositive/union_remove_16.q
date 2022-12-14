set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1;
set mapred.input.dir.recursive=true;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- SORT_QUERY_RESULTS
-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- and the results are written to a table using dynamic partitions.
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- on
-- This test demonstrates that this optimization works in the presence of dynamic partitions.
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1_n32, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1_n23(key string, val string) stored as textfile;
create table outputTbl1_n32(key string, `values` bigint) partitioned by (ds string) stored as rcfile ;

load data local inpath '../../data/files/T1.txt' into table inputTbl1_n23;

explain
insert overwrite table outputTbl1_n32 partition (ds)
SELECT *
FROM (
  SELECT key, count(1) as `values`, '1' as ds from inputTbl1_n23 group by key
  UNION ALL
  SELECT key, count(1) as `values`, '2' as ds from inputTbl1_n23 group by key
) a;

insert overwrite table outputTbl1_n32 partition (ds)
SELECT *
FROM (
  SELECT key, count(1) as `values`, '1' as ds from inputTbl1_n23 group by key
  UNION ALL
  SELECT key, count(1) as `values`, '2' as ds from inputTbl1_n23 group by key
) a;

desc formatted outputTbl1_n32;
show partitions outputTbl1_n32;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1_n32 where ds = '1';
select * from outputTbl1_n32 where ds = '2';
