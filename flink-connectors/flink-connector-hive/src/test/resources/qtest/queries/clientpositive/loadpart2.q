
create table hive_test ( col1 string ) partitioned by ( pcol1 string , pcol2 string) stored as textfile;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/test.dat' overwrite into table hive_test partition (pcol1='part1',pcol2='part1') ;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/test.dat' overwrite into table hive_test partition (pcol2='part2',pcol1='part2') ;
select * from hive_test where pcol1='part1' and pcol2='part1';
select * from hive_test where pcol1='part2' and pcol2='part2';



