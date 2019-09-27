create table tab_string(a string);

-- insert some data
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/string.txt" INTO TABLE tab_string;

select count(*) from tab_string;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_string;
