-- HIVE-5292 Join on decimal columns fails
-- SORT_QUERY_RESULTS

create table src_dec (key decimal(3,0), value string);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' into table src_dec;

select * from src_dec a join src_dec b on a.key=b.key+450;
