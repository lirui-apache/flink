CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T1.txt' INTO TABLE T1;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T2.txt' INTO TABLE T2;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T3.txt' INTO TABLE T3;

-- SORT_QUERY_RESULTS

FROM UNIQUEJOIN PRESERVE T1 a (a.key), PRESERVE T2 b (b.key), PRESERVE T3 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN T1 a (a.key), T2 b (b.key), T3 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN T1 a (a.key), T2 b (b.key-1), T3 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN PRESERVE T1 a (a.key, a.val), PRESERVE T2 b (b.key, b.val), PRESERVE T3 c (c.key, c.val)
SELECT a.key, a.val, b.key, b.val, c.key, c.val;

FROM UNIQUEJOIN PRESERVE T1 a (a.key), T2 b (b.key), PRESERVE T3 c (c.key)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN PRESERVE T1 a (a.key), T2 b(b.key)
SELECT a.key, b.key;

drop table if exists t1;

drop table if exists t2;

drop table if exists t3;
