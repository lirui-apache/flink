CREATE TABLE mytable(key STRING, value STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '9'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/string.txt' INTO TABLE mytable;

EXPLAIN
SELECT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(key, '\001', '^A'), '\0', '^@'), '\002', '^B'), value
FROM (
        SELECT key, sum(value) as value
        FROM mytable
        GROUP BY key
) a;

SELECT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(key, '\001', '^A'), '\0', '^@'), '\002', '^B'), value
FROM (
        SELECT key, sum(value) as value
        FROM mytable
        GROUP BY key
) a;

drop table if exists mytable;
