create function example_add10 as 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFAdd10';

create table t1(x int,y double);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T1.txt' into table t1;

explain select example_add10(x) as a,example_add10(y) as b from t1 order by a desc,b limit 10;

select example_add10(x) as a,example_add10(y) as b from t1 order by a desc,b limit 10;

drop table t1;
drop function if exists example_add10;
