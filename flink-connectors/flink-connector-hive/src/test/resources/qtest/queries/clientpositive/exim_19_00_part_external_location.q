set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_employee ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/test.dat"
	into table exim_employee partition (emp_country="in", emp_state="tn");	
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/test2.dat"
	into table exim_employee partition (emp_country="in", emp_state="ka");	
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee to 'ql/test/data/exports/exim_employee';
drop table exim_employee;

create database importer;
use importer;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/tablestore/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_employee;

import external table exim_employee 
	from 'ql/test/data/exports/exim_employee'
	location 'ql/test/data/tablestore/exim_employee';
describe extended exim_employee;	
show table extended like exim_employee;
show table extended like exim_employee partition (emp_country="in", emp_state="tn");
show table extended like exim_employee partition (emp_country="in", emp_state="ka");
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
select * from exim_employee;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_employee;
select * from exim_employee;
drop table exim_employee;

drop database importer;
