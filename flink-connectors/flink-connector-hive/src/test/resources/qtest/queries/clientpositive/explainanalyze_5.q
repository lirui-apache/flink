set hive.map.aggr=false;

set hive.stats.column.autogather=true;

drop table src_stats;

create table src_stats as select * from src;

explain analyze analyze table src_stats compute statistics;

explain analyze analyze table src_stats compute statistics for columns;

drop table src_multi2;

create table src_multi2 like src;

explain analyze insert overwrite table src_multi2 select subq.key, src.value from (select * from src union select * from src1)subq join src on subq.key=src.key;

select count(*) from (select * from src union select * from src1)subq;

insert overwrite table src_multi2 select subq.key, src.value from (select * from src union select * from src1)subq join src on subq.key=src.key;

describe formatted src_multi2;

drop table if exists src_multi2;
