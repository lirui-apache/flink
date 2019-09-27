-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors (
  number int,
  first_name string,
  last_name string)
STORED AS AVRO;

DESCRIBE doctors;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/doctors.avro' INTO TABLE doctors;

SELECT * FROM doctors;
drop table if exists doctors;
