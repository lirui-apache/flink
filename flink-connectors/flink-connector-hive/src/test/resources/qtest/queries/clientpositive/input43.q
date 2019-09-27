drop table tst_src1;
create table tst_src1 like src1;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table tst_src1 ;
select count(1) from tst_src1;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table tst_src1 ;
select count(1) from tst_src1;
drop table tst_src1;
