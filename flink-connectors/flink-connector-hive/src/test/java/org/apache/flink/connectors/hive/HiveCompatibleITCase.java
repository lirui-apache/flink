/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive;

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test hive query compatibility.
 */
public class HiveCompatibleITCase {

	private static HiveCatalog hiveCatalog;

	private static final String[] QUERIES = new String[]{
			"select x from foo order by x desc limit 1",
			"select x,count(y),max(y) from foo group by x",
			"select count(distinct i) from bar group by s",
			"select max(c) from (select x,count(y) as c from foo group by x) t1",
			"select count(x) from foo union all select i from bar",
			"select x,sum(y) as s from foo group by x having min(y)>1",
			"select s from foo join bar on foo.x=bar.i and foo.y=bar.i group by s order by s",
			"select * from foo join (select max(i) as m from bar) a on foo.y=a.m",
			"select * from foo left outer join bar on foo.y=bar.i",
			"select * from foo right outer join bar on foo.y=bar.i",
			"select * from foo full outer join bar on foo.y=bar.i",
			"select * from foo where y in (select i from bar)",
			"select * from foo left semi join bar on foo.y=bar.i",
			"select (select count(x) from foo where foo.y=bar.i) from bar",
			"select x from foo union select i from bar",
			"select avg(salary) over (partition by dep) as avgsal from employee",
			"select dep,name,salary from (select dep,name,salary,rank() over (partition by dep order by salary desc) as rnk from employee) a where rnk=1",
			"select salary,sum(cnt) over (order by salary)/sum(cnt) over (order by salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from (select salary,count(*) as cnt from employee group by salary) a",
			"select i from bar except select x from foo",
			"select x from foo intersect select i from bar",
			"select x,y,grouping__id,sum(1) from foo group by x,y grouping sets ((x,y),(x))",
			"select x,y,grouping(x),sum(1) from foo group by x,y grouping sets ((x,y),(x))",
			"select src.key,src.`[k].*` from src",
			"select * from (select a.value, a.* from (select * from src) a join (select * from src) b on a.key = b.key) t",
			"select * from bar where i in (1,2,3)",
			"select * from bar where i between 1 and 3",
			"select 'x' as key_new , split(value,',') as value_new from src ORDER BY key_new ASC, value_new[0] ASC limit 20",
			"select x from foo sort by x",
			"select x from foo cluster by x",
			"select x,y from foo distribute by abs(y)",
			"select x,y from foo distribute by y sort by x desc",
			"select f1.x,f1.y,f2.x,f2.y from (select * from foo order by x,y) f1 join (select * from foo order by x,y) f2",
			"select sum(x) as s1 from foo group by y having s1 > 2 and avg(x) < 4",
			"select sum(x) as s1,y as y1 from foo group by y having s1 > 2 and y1 < 4",
			"select x,col1 from (select x,array(1,2,3) as arr from foo) f lateral view explode(arr) tbl1 as col1",
			"select dep,count(1) from employee where salary<5000 and age>=38 and dep='Sales' group by dep",
			"select x,null as n from foo group by x,'a',null",
			"SELECT key, value FROM (SELECT key FROM src group by key) a lateral view explode(array(1, 2)) value as value",
			"select explode(array(1,2,3)) from foo",
			"select value from src where key=_UTF-8 0xE982B5E993AE",
			"SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1",
			"SELECT explode(map('key1', 100, 'key2', 200)) from src limit 2",
			"select key, value from src where key in (select key+18 from src) order by key",
			"select x from foo where x in (select key from src)",
			"select col1 from foo lateral view explode(array(x,y)) tbl1 as col1",
			"SELECT col1, col2 FROM nested LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1 LATERAL VIEW explode(s3.f12) tbl2 AS col2",
			"select * from foo where cast(x as double)<=0 order by cast(x as double)",
			"select (case when i>1 then 100 else split(s,',')[0] end) as a from bar",
			"select if(i>1,s,null) from bar",
			"select temp_add(x,y) from foo",
			"select default.temp_max(i) from bar",
			"select temp_explode(ai) from baz",
			"select col1 from baz lateral view default.temp_explode(ai) tbl1 as col1"
	};

	private static final String[] UPDATES = new String[]{
			"insert into dest select 0,y from foo sort by y",
			"insert into dest(y,x) select x,y from foo cluster by x",
			"insert into dest(y) select y from foo sort by y limit 1",
			"insert into destp select x,'0','00' from foo order by x limit 1",
			"insert overwrite table destp partition(p='0',q) select 1,`value` from src sort by `value`",
			"insert into dest select * from src",
			"insert overwrite table destp partition (p='-1',q='-1') if not exists select x from foo",
			"insert into destp partition(p='1',q) (x,q) select * from bar",
			"insert into destp partition(p='1',q) (q) select s from bar",
			"insert into destp partition(p,q) (p,x) select s,i from bar",
			"insert into destp partition (p,q) (q,x) values ('a',2)"
	};

	@BeforeClass
	public static void setup() {
		hiveCatalog = HiveTestUtils.createHiveCatalog();
		hiveCatalog.open();
	}

	@Test
	public void testHiveCompatible() throws Exception {
		// required by query like "src.`[k].*` from src"
		hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog(SqlDialect.HIVE);

		tableEnv.executeSql("create table foo (x int, y int)");
		tableEnv.executeSql("create table bar(i int, s string)");
		tableEnv.executeSql("create table baz(ai array<int>, d double)");
		tableEnv.executeSql("create table employee(id int,name string,dep string,salary int,age int)");
		tableEnv.executeSql("create table dest (x int, y int)");
		tableEnv.executeSql("create table destp (x int) partitioned by (p string, q string)");
		tableEnv.executeSql("alter table destp add partition (p='-1',q='-1')");
		tableEnv.executeSql("CREATE TABLE src (key STRING, `value` STRING)");
		tableEnv.executeSql("CREATE TABLE srcpart (key STRING, `value` STRING) PARTITIONED BY (ds STRING, hr STRING)");
		tableEnv.executeSql("CREATE TABLE nested (\n" +
				"  a int,\n" +
				"  s1 struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>,\n" +
				"  s2 struct<f7: string, f8: struct<f9 : boolean, f10: array<int>, f11: map<string, boolean>>>,\n" +
				"  s3 struct<f12: array<struct<f13:string, f14:int>>>,\n" +
				"  s4 map<string, struct<f15:int>>,\n" +
				"  s5 struct<f16: array<struct<f17:string, f18:struct<f19:int>>>>,\n" +
				"  s6 map<string, struct<f20:array<struct<f21:struct<f22:int>>>>>\n" +
				")");
		HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "foo")
				.addRow(new Object[]{1, 1})
				.addRow(new Object[]{2, 2})
				.addRow(new Object[]{3, 3})
				.addRow(new Object[]{4, 4})
				.addRow(new Object[]{5, 5})
				.commit();
		HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "bar")
				.addRow(new Object[]{1, "a"})
				.addRow(new Object[]{1, "aa"})
				.addRow(new Object[]{2, "b"})
				.commit();
		HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "baz")
				.addRow(new Object[]{Arrays.asList(1, 2, 3), 3.0})
				.commit();
		HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
				.addRow(new Object[]{"1", "val1"})
				.addRow(new Object[]{"2", "val2"})
				.addRow(new Object[]{"3", "val3"})
				.commit();
		HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "employee")
				.addRow(new Object[]{1, "A", "Management", 4500, 55})
				.addRow(new Object[]{2, "B", "Management", 4400, 61})
				.addRow(new Object[]{3, "C", "Management", 4000, 42})
				.addRow(new Object[]{4, "D", "Production", 3700, 35})
				.addRow(new Object[]{5, "E", "Production", 3500, 24})
				.addRow(new Object[]{6, "F", "Production", 3600, 28})
				.addRow(new Object[]{7, "G", "Production", 3800, 35})
				.addRow(new Object[]{8, "H", "Production", 4000, 52})
				.addRow(new Object[]{9, "I", "Service", 4100, 40})
				.addRow(new Object[]{10, "J", "Sales", 4300, 36})
				.addRow(new Object[]{11, "K", "Sales", 4100, 38})
				.commit();

		List<String> dqlToRun = new ArrayList<>(Arrays.asList(QUERIES));
		// add test cases specific to each version
		if (HiveVersionTestUtil.HIVE_220_OR_LATER) {
			dqlToRun.add("select weekofyear(current_timestamp()), dayofweek(current_timestamp()) from src limit 1");
		}
		// create functions
		tableEnv.executeSql("create function hiveudf as 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd'");
		tableEnv.executeSql("create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
		tableEnv.executeSql("create function myudtf as '" + MyUDTF.class.getName() + "'");
		dqlToRun.add("select default.hiveudf(x,y) from foo");
		dqlToRun.add("select hiveudtf(ai) from baz");
		dqlToRun.add("select col1,d from baz lateral view hiveudtf(ai) tbl1 as col1");
		dqlToRun.add("select col1,col2,d from baz lateral view hiveudtf(ai) tbl1 as col1 lateral view hiveudtf(ai) tbl2 as col2");
		dqlToRun.add("select col1 from foo lateral view myudtf(x,y) tbl1 as col1");
		// create temp functions
		tableEnv.executeSql("create temporary function temp_add as 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd'");
		tableEnv.executeSql("create temporary function temp_explode as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
		tableEnv.executeSql("create temporary function temp_max as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax'");

		// test explain
		runExplain(tableEnv, "explain insert into dest select * from foo");
		runExplain(tableEnv, "explain extended select * from foo");

//		runUpdate("insert overwrite table dest select * from bar", tableEnv);
//		runQuery("select x,count(y),max(y) from foo group by x", tableEnv);

		for (String query : dqlToRun) {
			runQuery(query, tableEnv);
		}
		for (String dml : UPDATES) {
			runUpdate(dml, tableEnv);
		}
		System.out.println("finished");
	}

	private void runExplain(TableEnvironment tableEnv, String sql) {
		TableResult tableResult = tableEnv.executeSql(sql);
		assertFalse(tableResult.getJobClient().isPresent());
		assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind());
	}

	private void runQuery(String query, TableEnvironment tableEnv) throws Exception {
		org.apache.flink.table.api.Table resultTable = tableEnv.sqlQuery(query);
		System.out.println(resultTable.explain());
		System.out.println(CollectionUtil.iteratorToList(resultTable.execute().collect()));
		System.out.println("Successfully executed SQL: " + query);
	}

	private void runUpdate(String dml, TableEnvironment tableEnv) throws Exception {
		System.out.println(tableEnv.explainSql(dml));
		tableEnv.executeSql(dml).await();
		System.out.println("Successfully executed DML: " + dml);
	}

	private TableEnvironment getTableEnvWithHiveCatalog(SqlDialect dialect) {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(dialect);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		if (dialect == SqlDialect.HIVE) {
			// automatically load hive module in hive-compatible mode
			// TODO: move this logic to table env impl
			HiveModule hiveModule = new HiveModule(hiveCatalog.getHiveVersion());
			CoreModule coreModule = CoreModule.INSTANCE;
			for (String loaded : tableEnv.listModules()) {
				tableEnv.unloadModule(loaded);
			}
			tableEnv.loadModule("hive", hiveModule);
			tableEnv.loadModule("core", coreModule);
		}
		return tableEnv;
	}

	/**
	 * A test UDTF that takes multiple parameters.
	 */
	public static class MyUDTF extends GenericUDTF {

		@Override
		public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
			return ObjectInspectorFactory.getStandardStructObjectInspector(
					Collections.singletonList("col1"),
					Collections.singletonList(PrimitiveObjectInspectorFactory.javaIntObjectInspector));
		}

		@Override
		public void process(Object[] args) throws HiveException {
			int x = (int) args[0];
			for (int i = 0; i < x; i++) {
				forward(i);
			}
		}

		@Override
		public void close() throws HiveException {
		}
	}
}
