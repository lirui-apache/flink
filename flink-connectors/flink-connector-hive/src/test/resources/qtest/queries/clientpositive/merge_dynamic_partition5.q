set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- this is to test the case where some dynamic partitions are merged and some are moved

create table srcpart_merge_dp like srcpart;

create table srcpart_merge_dp_rc like srcpart;
alter table srcpart_merge_dp_rc set fileformat RCFILE;

create table merge_dynamic_part like srcpart;
alter table merge_dynamic_part set fileformat RCFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);

insert overwrite table srcpart_merge_dp_rc partition (ds = '2008-04-08', hr) 
select key, value, hr from srcpart_merge_dp where ds = '2008-04-08';

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=200;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table merge_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc where ds = '2008-04-08';

insert overwrite table merge_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc where ds = '2008-04-08';

show partitions merge_dynamic_part;

select count(*) from merge_dynamic_part;

drop table if exists srcpart_merge_dp;

drop table if exists merge_dynamic_part;

drop table if exists srcpart_merge_dp_rc;
