-- test for loading into tables with the file with space in the name


CREATE TABLE load_file_with_space_in_the_name(name STRING, age INT);
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/person age.txt' INTO TABLE load_file_with_space_in_the_name;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/person+age.txt' INTO TABLE load_file_with_space_in_the_name;

drop table if exists load_file_with_space_in_the_name;
