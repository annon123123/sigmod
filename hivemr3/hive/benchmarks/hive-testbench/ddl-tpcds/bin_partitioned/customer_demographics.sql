create database if not exists ${DB};
use ${DB};

drop table if exists customer_demographics;

create table customer_demographics
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.customer_demographics;
