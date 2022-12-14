SET hive.vectorized.execution.enabled=true;

CREATE EXTERNAL TABLE kafka_table
(`__time` timestamp , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
WITH SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
TBLPROPERTIES
("kafka.topic" = "test-topic",
"kafka.bootstrap.servers"="localhost:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.JsonSerDe")
;

DESCRIBE EXTENDED kafka_table;

Select `__partition` , `__offset`,`__key`, `__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta FROM kafka_table;

Select count(*) FROM kafka_table;

Select `__partition`, `__offset`, `__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where `__timestamp` > 1533960760123;
Select `__partition`, `__offset` ,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where `__timestamp` > 533960760123;

Select `__partition`, `__offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
from kafka_table where (`__offset` > 7 and `__partition` = 0 and `__offset` <9 ) OR
`__offset` = 4 and `__partition` = 0 OR (`__offset` <= 1 and `__partition` = 0 and `__offset` > 0);

Select `__key`,`__partition`, `__offset`,`__time`, `page`, `user` from kafka_table where `__offset` = 5;

Select `__key`,`__partition`, `__offset`,`__time`, `page`, `user` from kafka_table where `__offset` < 5;

Select `__key`,`__partition`, `__offset`,`__time`, `page`, `user` from kafka_table where `__offset` > 5;

-- Timestamp filter

Select `__partition`, `__offset`, `user`  from kafka_table where
`__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '1' HOURS) ;

-- non existing partition
Select  count(*) from kafka_table where `__partition` = 1;

-- non existing offset
Select count(*) from kafka_table where `__offset` = 100;

-- less than non existing offset  and partition
Select count(*) from kafka_table where `__offset` <= 100 and `__partition` <= 100;

Drop table kafka_table_offsets;
create table kafka_table_offsets(partition_id int, max_offset bigint, insert_time timestamp);

insert overwrite table kafka_table_offsets select `__partition`, min(`__offset`) - 1, CURRENT_TIMESTAMP from kafka_table group by `__partition`, CURRENT_TIMESTAMP ;

-- check initial state is 0 for partition and 0 offsets
select partition_id, max_offset from kafka_table_offsets;

Drop table orc_kafka_table;
Create table orc_kafka_table (partition_id int, row_offset bigint, kafka_ts bigint,
 `__time` timestamp , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint
) stored as ORC;


From kafka_table ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id and ktable.`__offset` > offset_table.max_offset and  ktable.`__offset` < 3 )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;

-- should ingest only first 3 rows
select count(*) from  orc_kafka_table;

-- check max offset is 2
select partition_id, max_offset from kafka_table_offsets;

-- 3 rows form 0 to 2
select `partition_id`, `row_offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta from  orc_kafka_table;


-- insert the rest using inner join

From kafka_table ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id and ktable.`__offset` > offset_table.max_offset)
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;

-- check that max offset is 9
select partition_id, max_offset from kafka_table_offsets;

-- 10 rows
select count(*) from  orc_kafka_table;

-- no duplicate or missing data
select `partition_id`, `row_offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta from  orc_kafka_table;

-- LEFT OUTER JOIN if metadata is empty

Drop table kafka_table_offsets;
create table kafka_table_offsets(partition_id int, max_offset bigint, insert_time timestamp);

Drop table orc_kafka_table;
Create table orc_kafka_table (partition_id int, row_offset bigint, kafka_ts bigint,
 `__time` timestamp , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint
) stored as ORC;


From kafka_table ktable LEFT OUTER JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id and ktable.`__offset` > offset_table.max_offset )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;

select count(*) from  orc_kafka_table;

select partition_id, max_offset from kafka_table_offsets;

select `partition_id`, `row_offset`,`__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta from  orc_kafka_table;

-- using basic implementation of flat json probably to be removed
CREATE EXTERNAL TABLE kafka_table_2
(`__time` timestamp with local time zone , `page` string, `user` string, `language` string,
`country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
`anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
WITH SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
TBLPROPERTIES
("kafka.topic" = "test-topic",
"kafka.bootstrap.servers"="localhost:9092");

Select `__partition`, `__offset`, `__time`, `page`, `user`, `language`, `country`,`continent`, `namespace`, `newPage` ,
`unpatrolled` , `anonymous` , `robot` , added , deleted , delta
FROM kafka_table_2;

Select count(*) FROM kafka_table_2;

CREATE EXTERNAL TABLE wiki_kafka_avro_table
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "wiki_kafka_avro_table",
"kafka.bootstrap.servers"="localhost:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe",
'avro.schema.literal'='{
  "type" : "record",
  "name" : "Wikipedia",
  "namespace" : "org.apache.hive.kafka",
  "version": "1",
  "fields" : [ {
    "name" : "isrobot",
    "type" : "boolean"
  }, {
    "name" : "channel",
    "type" : "string"
  }, {
    "name" : "timestamp",
    "type" : "string"
  }, {
    "name" : "flags",
    "type" : "string"
  }, {
    "name" : "isunpatrolled",
    "type" : "boolean"
  }, {
    "name" : "page",
    "type" : "string"
  }, {
    "name" : "diffurl",
    "type" : "string"
  }, {
    "name" : "added",
    "type" : "long"
  }, {
    "name" : "comment",
    "type" : "string"
  }, {
    "name" : "commentlength",
    "type" : "long"
  }, {
    "name" : "isnew",
    "type" : "boolean"
  }, {
    "name" : "isminor",
    "type" : "boolean"
  }, {
    "name" : "delta",
    "type" : "long"
  }, {
    "name" : "isanonymous",
    "type" : "boolean"
  }, {
    "name" : "user",
    "type" : "string"
  }, {
    "name" : "deltabucket",
    "type" : "double"
  }, {
    "name" : "deleted",
    "type" : "long"
  }, {
    "name" : "namespace",
    "type" : "string"
  } ]
}'
);

describe extended wiki_kafka_avro_table;


select cast (`__timestamp` as timestamp) as kafka_record_ts, `__partition`, `__offset`, `timestamp`, `user`,
 `page`, `deleted`, `deltabucket`, `isanonymous`, `commentlength` from wiki_kafka_avro_table;

select count(*) from wiki_kafka_avro_table;

select count(distinct `user`) from  wiki_kafka_avro_table;

select sum(deltabucket), min(commentlength) from wiki_kafka_avro_table;

select cast (`__timestamp` as timestamp) as kafka_record_ts, `__timestamp` as kafka_record_ts_long,
`__partition`, `__key`, `__offset`, `timestamp`, `user`, `page`, `deleted`, `deltabucket`,
`isanonymous`, `commentlength` from wiki_kafka_avro_table where `__timestamp` > 1534750625090;


CREATE EXTERNAL TABLE kafka_table_insert
(c_name string, c_int int, c_float float)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
WITH SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
TBLPROPERTIES
("kafka.topic" = "test-topic-write-json",
"kafka.bootstrap.servers"="localhost:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.JsonSerDe")
;

insert into table kafka_table_insert (c_name,c_int, c_float,`__key`, `__partition`, `__offset`, `__timestamp`)
values ('test1',5, 4.999,'key',null ,-1,1536449552290);

insert into table kafka_table_insert (c_name,c_int, c_float, `__key`, `__partition`, `__offset`, `__timestamp`)
values ('test2',15, 14.9996666, null ,null ,-1,1536449552285);

select * from kafka_table_insert;


insert into table wiki_kafka_avro_table select
isrobot as isrobot, channel as channel,`timestamp` as `timestamp`,  flags as flags,  isunpatrolled as isunpatrolled, page as page,
diffurl as diffurl, added as added, comment as comment, commentlength as commentlength, isnew as isnew, isminor as isminor,
delta as delta, isanonymous as isanonymous, `user` as `user`,  deltabucket as detlabucket, deleted as deleted, namespace as namespace,
`__key`, `__partition`, -1 as `__offset`,`__timestamp`
from wiki_kafka_avro_table;

select cast ((`__timestamp`/1000) as timestamp) as kafka_record_ts, `__partition`, `__offset`, `timestamp`, `user`,
`page`, `deleted`, `deltabucket`, `isanonymous`, `commentlength` from wiki_kafka_avro_table;

select `__key`, count(1)  FROM wiki_kafka_avro_table group by `__key` order by `__key`;

select `__timestamp`, count(1) from wiki_kafka_avro_table group by `__timestamp` order by `__timestamp`;


CREATE EXTERNAL TABLE kafka_table_csv
(c_name string, c_int int, c_float float)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "test-topic-write-csv",
"kafka.bootstrap.servers"="localhost:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.OpenCSVSerde");

ALTER TABLE kafka_table_csv SET TBLPROPERTIES ("hive.kafka.optimistic.commit"="false", "kafka.write.semantic"="EXACTLY_ONCE");
insert into table kafka_table_csv select c_name, c_int, c_float, `__key`, `__partition`, -1 as `__offset`, `__timestamp` from kafka_table_insert;

insert into table kafka_table_csv (c_name,c_int, c_float,`__key`, `__partition`, `__offset`, `__timestamp`)
values ('test4',-5, -4.999,'key-2',null ,-1,1536449552291);

insert into table kafka_table_csv (c_name,c_int, c_float, `__key`, `__partition`, `__offset`, `__timestamp`)
values ('test5',-15, -14.9996666, 'key-3' ,null ,-1,1536449552284);

select * from kafka_table_csv;
select distinct `__key`, c_name from kafka_table_csv;

SET hive.vectorized.execution.enabled=false ;
explain extended select distinct `__offset`, cast(`__timestamp` as timestamp ) , `__key` from wiki_kafka_avro_table;
select distinct `__offset`, cast(`__timestamp` as timestamp ) , `__key` from wiki_kafka_avro_table;

SET hive.vectorized.execution.enabled=true ;
explain extended select distinct `__offset`, cast(`__timestamp` as timestamp ) , `__key` from wiki_kafka_avro_table;
select distinct `__offset`, cast(`__timestamp` as timestamp ) , `__key` from wiki_kafka_avro_table;
