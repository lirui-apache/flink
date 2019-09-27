create table timetest_parquet(t timestamp) stored as parquet;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/parquet_external_time.parq' into table timetest_parquet;

select * from timetest_parquet;