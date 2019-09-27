
CREATE TABLE tstparttbl(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE tstparttbl PARTITION (ds='2009-04-09');


CREATE TABLE tstparttbl2(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE tstparttbl2 PARTITION (ds='2009-04-09');

explain
select u.* from
(
  select key, value from tstparttbl x where x.ds='2009-04-05'
    union all  
  select key, value from tstparttbl2 y where y.ds='2009-04-09'
)u;

select u.* from
(
  select key, value from tstparttbl x where x.ds='2009-04-05'
    union all  
  select key, value from tstparttbl2 y where y.ds='2009-04-09'
)u;





drop table if exists tstparttbl;

drop table if exists tstparttbl2;
