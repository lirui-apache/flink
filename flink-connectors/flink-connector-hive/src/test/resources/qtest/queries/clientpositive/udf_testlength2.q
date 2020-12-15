set hive.fetch.task.conversion=more;

create function testlength2 AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength2';

SELECT testlength2(src.value) FROM src;

drop function if exists testlength2;
