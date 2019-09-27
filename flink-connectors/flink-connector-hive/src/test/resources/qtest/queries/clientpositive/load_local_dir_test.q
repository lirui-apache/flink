
create table load_local (id INT);

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/ext_test/' into table load_local;

select * from load_local;
