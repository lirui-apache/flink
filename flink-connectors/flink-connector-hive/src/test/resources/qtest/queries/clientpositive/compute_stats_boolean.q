create table tab_bool(a boolean);

-- insert some data
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bool.txt" INTO TABLE tab_bool;

select count(*) from tab_bool;

-- compute statistical summary of data
select compute_stats(a, 16) from tab_bool;
