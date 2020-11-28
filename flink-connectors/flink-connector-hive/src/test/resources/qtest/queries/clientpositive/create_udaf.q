CREATE FUNCTION test_max AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax';

CREATE TABLE dest1(col INT);

FROM src INSERT OVERWRITE TABLE dest1 SELECT test_max(length(src.value));

SELECT dest1.* FROM dest1;

-- cover all the other value types:
SELECT test_max(CAST(length(src.value) AS SMALLINT)) FROM src;
SELECT test_max(CAST(length(src.value) AS BIGINT)) FROM src;
SELECT test_max(CAST(length(src.value) AS DOUBLE)) FROM src;
SELECT test_max(CAST(length(src.value) AS FLOAT)) FROM src;
SELECT test_max(substr(src.value,5)) FROM src;

DROP FUNCTION test_max;

drop table if exists dest1;
