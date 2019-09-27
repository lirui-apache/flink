create table load_overwrite like src;

insert overwrite table load_overwrite select * from src;
show table extended like load_overwrite;
select count(*) from load_overwrite;


LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table load_overwrite;
show table extended like load_overwrite;
select count(*) from load_overwrite;


LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' overwrite into table load_overwrite;
show table extended like load_overwrite;
select count(*) from load_overwrite;
