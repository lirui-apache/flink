explain extended create table t as select * from src union all select * from src;

create table t as select * from src union all select * from src;

select count(1) from t;

desc formatted t;

create table tt as select * from t union all select * from src;

desc formatted tt;

drop table tt;

create table tt as select * from src union all select * from t;

desc formatted tt;

create table t1 like src;
create table t2 like src;

from (select * from src union all select * from src)s
insert overwrite table t1 select *
insert overwrite table t2 select *;

desc formatted t1;
desc formatted t2;

select count(1) from t1;

drop table if exists t1;

drop table if exists t2;

drop table if exists t;

drop table if exists tt;
