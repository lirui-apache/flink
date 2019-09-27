-- SORT_QUERY_RESULTS

CREATE TABLE joinone(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/in5.txt' INTO TABLE joinone;

CREATE TABLE joinTwo(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/in6.txt' INTO TABLE joinTwo;

SELECT * FROM joinone JOIN joinTwo ON(joinone.key2=joinTwo.key2);
