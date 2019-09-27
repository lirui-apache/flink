
create table load_overwrite (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/load_overwrite';
create table load_overwrite2 (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/load2_overwrite2';

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table load_overwrite;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv2.txt' into table load_overwrite;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv3.txt' into table load_overwrite;

show table extended like load_overwrite;
desc extended load_overwrite;
select count(*) from load_overwrite;

load data inpath '${system:test.tmp.dir}/load_overwrite/kv*.txt' overwrite into table load_overwrite2;

show table extended like load_overwrite2;
desc extended load_overwrite2;
select count(*) from load_overwrite2;

load data inpath '${system:test.tmp.dir}/load2_*' overwrite into table load_overwrite;
show table extended like load_overwrite;
select count(*) from load_overwrite;
