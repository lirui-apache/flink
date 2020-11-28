--
-- Table src
--

DROP TABLE IF EXISTS src;

CREATE TABLE src (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src;

--
-- Table src1
--
DROP TABLE IF EXISTS src1;

CREATE TABLE src1 (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv3.txt" INTO TABLE src1;

--
-- Table src_json
--
DROP TABLE IF EXISTS src_json;

CREATE TABLE src_json (json STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/json.txt" INTO TABLE src_json;

--
-- Table src_sequencefile
--
DROP TABLE IF EXISTS src_sequencefile;

CREATE TABLE src_sequencefile (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.seq" INTO TABLE src_sequencefile;

--
-- Table src_thrift
--
DROP TABLE IF EXISTS src_thrift;

CREATE TABLE src_thrift
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES (
        'serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
        'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
    STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/complex.seq" INTO TABLE src_thrift;

--
-- Table srcpart
--
DROP TABLE IF EXISTS srcpart;

CREATE TABLE srcpart (key STRING COMMENT 'default', value STRING COMMENT 'default')
    PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="12");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="12");

ANALYZE TABLE srcpart PARTITION(ds, hr) COMPUTE STATISTICS;

ANALYZE TABLE srcpart PARTITION(ds, hr) COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table alltypesorc
--
DROP TABLE IF EXISTS alltypesorc;
CREATE TABLE alltypesorc(
                            ctinyint TINYINT,
                            csmallint SMALLINT,
                            cint INT,
                            cbigint BIGINT,
                            cfloat FLOAT,
                            cdouble DOUBLE,
                            cstring1 STRING,
                            cstring2 STRING,
                            ctimestamp1 TIMESTAMP,
                            ctimestamp2 TIMESTAMP,
                            cboolean1 BOOLEAN,
                            cboolean2 BOOLEAN)
    STORED AS ORC;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/alltypesorc"
OVERWRITE INTO  TABLE alltypesorc;

--
-- Table primitives
--

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090101.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=1);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090201.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=2);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090301.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090401.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=4);

--
-- Function qtest_get_java_boolean
--
DROP FUNCTION IF EXISTS qtest_get_java_boolean;
CREATE FUNCTION qtest_get_java_boolean AS 'org.apache.flink.connectors.hive.GenericUDFTestGetJavaBoolean';


-- It seems these dest tables should be created by each qfile

--
-- Table dest1
--
DROP TABLE IF EXISTS dest1;

-- CREATE TABLE dest1 (key STRING, value STRING);

--
-- Table dest2
--
DROP TABLE IF EXISTS dest2;

-- CREATE TABLE dest2 (key STRING, value STRING);

--
-- Table dest3
--
DROP TABLE IF EXISTS dest3;

-- CREATE TABLE dest3 (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING)
-- ALTER TABLE dest3 ADD PARTITION (ds='2008-04-08',hr='12');

--
-- Table dest4
--
DROP TABLE IF EXISTS dest4;

-- CREATE TABLE dest4 (key STRING, value STRING);

--
-- Table dest4_sequencefile
--
DROP TABLE IF EXISTS dest4_sequencefile;

-- CREATE TABLE dest4_sequencefile (key STRING, value STRING) STORED AS SEQUENCEFILE;


--
-- CBO tables
--

drop table if exists cbo_t1;
drop table if exists cbo_t2;
drop table if exists cbo_t3;
drop table if exists src_cbo;
drop table if exists part;
drop table if exists lineitem;

create table cbo_t1(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table cbo_t2(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table cbo_t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath "${hiveconf:test.data.dir}/cbo_t1.txt" into table cbo_t1 partition (dt='2014');
load data local inpath "${hiveconf:test.data.dir}/cbo_t2.txt" into table cbo_t2 partition (dt='2014');
load data local inpath "${hiveconf:test.data.dir}/cbo_t3.txt" into table cbo_t3;

CREATE TABLE part(
                     p_partkey INT,
                     p_name STRING,
                     p_mfgr STRING,
                     p_brand STRING,
                     p_type STRING,
                     p_size INT,
                     p_container STRING,
                     p_retailprice DOUBLE,
                     p_comment STRING
);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/part_tiny.txt" overwrite into table part;

CREATE TABLE lineitem (L_ORDERKEY      INT,
                       L_PARTKEY       INT,
                       L_SUPPKEY       INT,
                       L_LINENUMBER    INT,
                       L_QUANTITY      DOUBLE,
                       L_EXTENDEDPRICE DOUBLE,
                       L_DISCOUNT      DOUBLE,
                       L_TAX           DOUBLE,
                       L_RETURNFLAG    STRING,
                       L_LINESTATUS    STRING,
                       l_shipdate      STRING,
                       L_COMMITDATE    STRING,
                       L_RECEIPTDATE   STRING,
                       L_SHIPINSTRUCT  STRING,
                       L_SHIPMODE      STRING,
                       L_COMMENT       STRING)
    ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/lineitem.txt" OVERWRITE INTO TABLE lineitem;

create table src_cbo as select * from src;
