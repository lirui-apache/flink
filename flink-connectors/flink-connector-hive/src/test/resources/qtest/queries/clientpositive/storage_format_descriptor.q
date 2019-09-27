set hive.mapred.mode=nonstrict;
DROP TABLE stored_as_custom_text_serde;
CREATE TABLE stored_as_custom_text_serde(key string, value string) STORED AS customtextserde;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE stored_as_custom_text_serde;
SELECT * FROM stored_as_custom_text_serde ORDER BY key, value;
DROP TABLE stored_as_custom_text_serde;
