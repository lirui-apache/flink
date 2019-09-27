explain create table abc(mydata uniontype<int,double,array<string>,struct<a:int,b:string>>,
strct struct<a:int, b:string, c:string>);

create table abc(mydata uniontype<int,double,array<string>,struct<a:int,b:string>>,
strct struct<a:int, b:string, c:string>);

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/union_input.txt'
overwrite into table abc;

SELECT * FROM abc;

drop table if exists abc;
