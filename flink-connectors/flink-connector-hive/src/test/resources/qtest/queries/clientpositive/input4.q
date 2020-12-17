CREATE TABLE INPUT4(KEY STRING, VALUE STRING) STORED AS TEXTFILE;
EXPLAIN
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE INPUT4;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE INPUT4;

SELECT Input4Alias.VALUE, Input4Alias.KEY FROM INPUT4 AS Input4Alias;


drop table if exists input4;
