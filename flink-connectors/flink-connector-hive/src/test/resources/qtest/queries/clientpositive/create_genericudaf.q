set hive.mapred.mode=nonstrict;

CREATE FUNCTION test_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';

SELECT
    test_avg(1),
    test_avg(substr(value,5))
FROM src;

DROP FUNCTIOn test_avg;
