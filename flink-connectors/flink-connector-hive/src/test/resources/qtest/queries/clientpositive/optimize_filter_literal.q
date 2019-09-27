set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.join.emit.interval=100000;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.vectorized.execution.enabled=true;

-- SORT_QUERY_RESULTS

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS ORCFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

analyze table tab_part partition (ds='2008-04-08') compute statistics for columns;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS ORCFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

analyze table tab partition (ds='2008-04-08') compute statistics for columns;

set hive.join.emit.interval=2;

set mapred.reduce.tasks=3;

select * from
(select * from tab where tab.key = 0)a
full outer join
(select * from tab_part where tab_part.key = 98)b join tab_part c on a.key = b.key and b.key = c.key;


drop table if exists tab;

drop table if exists srcbucket_mapjoin_part;

drop table if exists srcbucket_mapjoin;

drop table if exists tab_part;
