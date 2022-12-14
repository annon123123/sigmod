SET hive.vectorized.execution.enabled=false;

-- This qfile tests whether equivalent DPP sink works are combined.
-- When combined, one DPP sink operator will have multiple target columns/works.

-- SORT_QUERY_RESULTS

create table part1(key string, value string) partitioned by (p string, q string);
insert into table part1 partition (p='1', q='1') values ('1','1'), ('2','2');
insert into table part1 partition (p='1', q='2') values ('3','3'), ('4','4');
insert into table part1 partition (p='2', q='1') values ('5','5'), ('6','6');
insert into table part1 partition (p='2', q='2') values ('7','7'), ('8','8');

create table part2_n1(key string, value string) partitioned by (p string, q string);
insert into table part2_n1 partition (p='3', q='3') values ('a','a'), ('b','b');
insert into table part2_n1 partition (p='3', q='4') values ('c','c'), ('d','d');
insert into table part2_n1 partition (p='4', q='3') values ('e','e'), ('f','f');
insert into table part2_n1 partition (p='4', q='4') values ('g','g'), ('h','h');

-- dpp works should be combined
explain
select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.p=src.key);

select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.p=src.key);

-- verify result

select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.p=src.key);


-- dpp works should be combined
explain
select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.q=src.key);

select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.q=src.key);

-- verify result

select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.q=src.key);


-- target works are already combined
explain
select * from
  (select part1.key, part1.value from part1 join src on part1.q=src.key) a
union all
  (select part1.key, part1.value from part1 join src on part1.q=src.key);

select * from
  (select part1.key, part1.value from part1 join src on part1.q=src.key) a
union all
  (select part1.key, part1.value from part1 join src on part1.q=src.key);

-- dpp works shouldn't be combined
explain
select * from
  (select part1.key, part1.value from part1 join src on part1.p=src.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.p=src.value);

-- dpp works shouldn't be combined
explain
select * from
  (select part1.key, part1.value from part1 join src on part1.p=upper(src.key)) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join src on part2_n1.p=src.key);

-- dpp works should be combined
explain
with top as
(select key from src order by key limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.q=top.key);
  
with top as
(select key from src order by key limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.q=top.key);
  
-- verify result

with top as
(select key from src order by key limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.q=top.key);
  

-- dpp works should be combined
explain
with top as
(select key, value from src order by key, value limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.p=top.key and part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.p=top.key and part2_n1.q=top.key);

with top as
(select key, value from src order by key, value limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.p=top.key and part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.p=top.key and part2_n1.q=top.key);

-- verify result

with top as
(select key, value from src order by key, value limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.p=top.key and part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.p=top.key and part2_n1.q=top.key);


-- dpp works shouldn't be combined
explain
with top as
(select key, value from src order by key, value limit 200)
select * from
  (select part1.key, part1.value from part1 join top on part1.p=top.key and part1.q=top.key) a
union all
  (select part2_n1.key, part2_n1.value from part2_n1 join top on part2_n1.p=top.key and part2_n1.q=top.value);

-- The following test case makes sure target map works can read from multiple DPP sinks,
-- when the DPP sinks have different target lists
-- see HIVE-18111

create table foo_n2(key string);
insert into table foo_n2 values ('1'),('2');

set hive.cbo.enable = false;

explain
select p from part2_n1 where p in (select max(key) from foo_n2)
union all
select p from part1 where p in (select max(key) from foo_n2 union all select min(key) from foo_n2);

select p from part2_n1 where p in (select max(key) from foo_n2)
union all
select p from part1 where p in (select max(key) from foo_n2 union all select min(key) from foo_n2);

drop table foo_n2;
drop table part1;
drop table part2_n1;
