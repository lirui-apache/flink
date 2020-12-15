-- user-added aggregates should be usable as windowing functions
create function mysum as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum';

select sum(key) over (), mysum(key) over () from src limit 1;

drop function if exists mysum;
