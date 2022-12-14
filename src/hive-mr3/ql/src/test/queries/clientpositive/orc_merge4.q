set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.orcfile.stripe.level=true;

DROP TABLE orcfile_merge3a;
DROP TABLE orcfile_merge3b;

CREATE TABLE orcfile_merge3a (key int, value string) 
    PARTITIONED BY (ds string) STORED AS ORC;
CREATE TABLE orcfile_merge3b (key int, value string) STORED AS TEXTFILE;

set hive.merge.mapfiles=false;
INSERT OVERWRITE TABLE orcfile_merge3a PARTITION (ds='1')
    SELECT * FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge3a/ds=1/;

set hive.merge.mapfiles=true;
INSERT OVERWRITE TABLE orcfile_merge3a PARTITION (ds='1')
    SELECT * FROM src;

INSERT OVERWRITE TABLE orcfile_merge3a PARTITION (ds='2')
    SELECT * FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge3a/ds=1/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge3a/ds=2/;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
EXPLAIN INSERT OVERWRITE TABLE orcfile_merge3b
    SELECT key, value FROM orcfile_merge3a;
INSERT OVERWRITE TABLE orcfile_merge3b
    SELECT key, value FROM orcfile_merge3a;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3a
) t;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=100;
set mapref.min.split.size=1;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3b
) t;

DROP TABLE orcfile_merge3a;
DROP TABLE orcfile_merge3b;
