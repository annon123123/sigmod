create database if not exists ${DB};
use ${DB};

drop table if exists customer_address;

create table customer_address
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.customer_address;
