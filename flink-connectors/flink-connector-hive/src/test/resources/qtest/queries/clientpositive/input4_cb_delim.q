CREATE TABLE INPUT4_CB(KEY STRING, VALUE STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\002' LINES TERMINATED BY '\012' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1_cb.txt' INTO TABLE INPUT4_CB;
SELECT INPUT4_CB.VALUE, INPUT4_CB.KEY FROM INPUT4_CB;

