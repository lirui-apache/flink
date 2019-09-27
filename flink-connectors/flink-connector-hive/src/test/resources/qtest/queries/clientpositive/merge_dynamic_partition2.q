set hive.strict.checks.bucketing=false;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table srcpart_merge_dp like srcpart;

create table merge_dynamic_part like srcpart;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket0.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/srcbucket1.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);


set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat; 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=3000;
set hive.exec.compress.output=false;
set hive.optimize.sort.dynamic.partition=false;

explain
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';

show table extended like `merge_dynamic_part`;


drop table if exists srcpart_merge_dp;

drop table if exists merge_dynamic_part;
