set hive.default.fileformat=ORC;
create table orc_staging (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp);
create table orc_test (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) partitioned by (ds string);

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/orc_split_elim.orc' into table orc_staging;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_staging/;

load data inpath '${hiveconf:hive.metastore.warehouse.dir}/orc_staging/orc_split_elim.orc' into table orc_test partition (ds='10');
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/orc_split_elim.orc' into table orc_test partition (ds='10');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=10/;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/orc_split_elim.orc' overwrite into table orc_staging;
load data inpath '${hiveconf:hive.metastore.warehouse.dir}/orc_staging/' overwrite into table orc_test partition (ds='10');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_test/ds=10/;

drop table if exists orc_staging;

drop table if exists orc_test;
