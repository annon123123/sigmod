create database if not exists ${DB};
use ${DB};

drop table if exists date_dim;

create table date_dim
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.date_dim;
