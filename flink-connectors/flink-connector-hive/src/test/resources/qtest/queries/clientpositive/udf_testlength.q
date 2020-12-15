set hive.fetch.task.conversion=more;

create function testlength AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength';

SELECT testlength(src.value) FROM src;

drop function if exists testlength;
