add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

create function explode2 AS 'org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFExplode2';

-- SORT_QUERY_RESULTS

EXPLAIN SELECT col1, col2 FROM src LATERAL VIEW explode2(array(1,2,3)) myTable AS col1, col2 group by col1, col2 LIMIT 3;

SELECT col1, col2 FROM src LATERAL VIEW explode2(array(1,2,3)) myTable AS col1, col2 group by col1, col2 LIMIT 3;

drop function if exists explode2;
