create database if not exists ${DB};
use ${DB};

drop table if exists item;

create table item
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.item;
