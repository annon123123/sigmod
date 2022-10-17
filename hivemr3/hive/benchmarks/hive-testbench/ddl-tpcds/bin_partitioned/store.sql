create database if not exists ${DB};
use ${DB};

drop table if exists store;

create table store
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.store;
