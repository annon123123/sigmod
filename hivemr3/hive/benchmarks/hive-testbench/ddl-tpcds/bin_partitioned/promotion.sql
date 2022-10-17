create database if not exists ${DB};
use ${DB};

drop table if exists promotion;

create table promotion
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.promotion;
