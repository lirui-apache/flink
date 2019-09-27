set hive.strict.checks.bucketing=false;





create table smb_bucket_1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE; 
create table smb_bucket_2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE; 
create table smb_bucket_3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/smbbucket_1.rc' overwrite into table smb_bucket_1;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/smbbucket_2.rc' overwrite into table smb_bucket_2;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/smbbucket_3.rc' overwrite into table smb_bucket_3;
 
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.cbo.enable=false;
-- SORT_QUERY_RESULTS

explain
select /*+mapjoin(a)*/ * from smb_bucket_1 a join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(a)*/ * from smb_bucket_1 a join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(a)*/ * from smb_bucket_1 a left outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(a)*/ * from smb_bucket_1 a left outer join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(a)*/ * from smb_bucket_1 a right outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(a)*/ * from smb_bucket_1 a right outer join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(a)*/ * from smb_bucket_1 a full outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(a)*/ * from smb_bucket_1 a full outer join smb_bucket_3 b on a.key = b.key;


explain
select /*+mapjoin(b)*/ * from smb_bucket_1 a join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket_1 a join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(b)*/ * from smb_bucket_1 a left outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket_1 a left outer join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(b)*/ * from smb_bucket_1 a right outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket_1 a right outer join smb_bucket_3 b on a.key = b.key;

explain
select /*+mapjoin(b)*/ * from smb_bucket_1 a full outer join smb_bucket_3 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket_1 a full outer join smb_bucket_3 b on a.key = b.key;

 




drop table if exists smb_bucket_1;

drop table if exists smb_bucket_2;

drop table if exists smb_bucket_3;
