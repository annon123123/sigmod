create database if not exists ${DB};
use ${DB};

drop table if exists warehouse;

create table warehouse
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.warehouse;
