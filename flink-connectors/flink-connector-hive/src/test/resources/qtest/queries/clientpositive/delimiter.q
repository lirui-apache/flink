create table impressions (imp string, msg string)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/in7.txt' INTO TABLE impressions;

select * from impressions;

select imp,msg from impressions;

drop table impressions;