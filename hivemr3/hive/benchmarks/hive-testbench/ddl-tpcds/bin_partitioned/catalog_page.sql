create database if not exists ${DB};
use ${DB};

drop table if exists catalog_page;

create table catalog_page
stored as ${FILE}
TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')
as select * from ${SOURCE}.catalog_page;
