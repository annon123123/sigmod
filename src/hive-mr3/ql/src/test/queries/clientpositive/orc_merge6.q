set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;

-- SORT_QUERY_RESULTS

-- orc file merge tests for static partitions
create table orc_merge5_n4 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) stored as orc;
create table orc_merge5a_n1 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) partitioned by (year string, hour int) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5_n4;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=50000;
SET hive.optimize.index.filter=true;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.compute.splits.in.am=true;
set tez.grouping.min-size=1000;
set tez.grouping.max-size=50000;

-- 3 mappers
explain insert overwrite table orc_merge5a_n1 partition (year="2000",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
insert overwrite table orc_merge5a_n1 partition (year="2000",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
insert overwrite table orc_merge5a_n1 partition (year="2001",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;

-- 3 files total
analyze table orc_merge5a_n1 partition(year="2000",hour=24) compute statistics noscan;
analyze table orc_merge5a_n1 partition(year="2001",hour=24) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2000/hour=24/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2001/hour=24/;
show partitions orc_merge5a_n1;
select * from orc_merge5a_n1;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-- 3 mappers
explain insert overwrite table orc_merge5a_n1 partition (year="2000",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
insert overwrite table orc_merge5a_n1 partition (year="2000",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
insert overwrite table orc_merge5a_n1 partition (year="2001",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;

-- 1 file after merging
analyze table orc_merge5a_n1 partition(year="2000",hour=24) compute statistics noscan;
analyze table orc_merge5a_n1 partition(year="2001",hour=24) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2000/hour=24/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2001/hour=24/;
show partitions orc_merge5a_n1;
select * from orc_merge5a_n1;

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

insert overwrite table orc_merge5a_n1 partition (year="2000",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
insert overwrite table orc_merge5a_n1 partition (year="2001",hour=24) select userid,string1,subtype,decimal1,ts from orc_merge5_n4 where userid<=13;
analyze table orc_merge5a_n1 partition(year="2000",hour=24) compute statistics noscan;
analyze table orc_merge5a_n1 partition(year="2001",hour=24) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2000/hour=24/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2001/hour=24/;
show partitions orc_merge5a_n1;
select * from orc_merge5a_n1;

set hive.merge.orcfile.stripe.level=true;
explain alter table orc_merge5a_n1 partition(year="2000",hour=24) concatenate;
alter table orc_merge5a_n1 partition(year="2000",hour=24) concatenate;
alter table orc_merge5a_n1 partition(year="2001",hour=24) concatenate;

-- 1 file after merging
analyze table orc_merge5a_n1 partition(year="2000",hour=24) compute statistics noscan;
analyze table orc_merge5a_n1 partition(year="2001",hour=24) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2000/hour=24/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n1/year=2001/hour=24/;
show partitions orc_merge5a_n1;
select * from orc_merge5a_n1;

