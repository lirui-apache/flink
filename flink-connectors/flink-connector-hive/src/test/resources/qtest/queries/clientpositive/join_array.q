create table tinyA(a bigint, b bigint) stored as textfile;
create table tinyB(a bigint, bList array<int>) stored as textfile;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tiny_a.txt' into table tinyA;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/tiny_b.txt' into table tinyB;

-- SORT_QUERY_RESULTS

select * from tinyA;
select * from tinyB;

select tinyB.a, tinyB.bList from tinyB full outer join tinyA on tinyB.a = tinyA.a;
