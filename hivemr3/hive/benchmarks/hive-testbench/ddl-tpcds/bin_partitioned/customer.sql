create database if not exists ${DB};
use ${DB};

drop table if exists customer;

create table customer
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.customer;
