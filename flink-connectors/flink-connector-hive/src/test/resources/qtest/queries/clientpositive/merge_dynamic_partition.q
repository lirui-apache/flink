set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- SORT_QUERY_RESULTS

create table srcpart_merge_dp like srcpart;

create table merge_dynamic_part like srcpart;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=1000000000;
set hive.optimize.sort.dynamic.partition=false;
explain
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';

select * from merge_dynamic_part;
show table extended like `merge_dynamic_part`;


set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1000000000;
explain
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr=11) select key, value from srcpart_merge_dp where ds='2008-04-08';
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr=11) select key, value from srcpart_merge_dp where ds='2008-04-08';

select * from merge_dynamic_part;
show table extended like `merge_dynamic_part`;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1000000000;
explain
insert overwrite table merge_dynamic_part partition (ds, hr) select key, value, ds, hr from srcpart_merge_dp where ds='2008-04-08' and hr=11;
insert overwrite table merge_dynamic_part partition (ds, hr) select key, value, ds, hr from srcpart_merge_dp where ds='2008-04-08' and hr=11;;

select * from merge_dynamic_part;
show table extended like `merge_dynamic_part`;


drop table if exists srcpart_merge_dp;

drop table if exists merge_dynamic_part;
