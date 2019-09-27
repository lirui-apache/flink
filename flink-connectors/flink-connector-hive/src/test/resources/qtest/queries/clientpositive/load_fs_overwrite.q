--HIVE 6209

drop table target;
drop table temp;

create table target (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/target';
create table temp (key string, value string) stored as textfile location 'file:${system:test.tmp.dir}/temp';

set fs.pfile.impl.disable.cache=false;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table temp;
load data inpath '${system:test.tmp.dir}/temp/kv1.txt' overwrite into table target;
select count(*) from target;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv2.txt' into table temp;
load data inpath '${system:test.tmp.dir}/temp/kv2.txt' overwrite into table target;
select count(*) from target;

drop table target;
drop table temp;