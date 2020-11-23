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
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.util.CollectionUtil;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test hive query compatibility.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveCompatibleITCase {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	private static HiveCatalog hiveCatalog;
	private static HiveMetastoreClientWrapper hmsClient;

	@BeforeClass
	public static void setup() {
		HiveConf hiveConf = hiveShell.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
		hmsClient = HiveMetastoreClientFactory.create(hiveConf, HiveShimLoader.getHiveVersion());
	}

	@Test
	public void testHiveCompatible() throws Exception {
		// required by query like "src.`[k].*` from src"
		hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");

		hiveShell.execute("create table foo (x int, y int)");
		hiveShell.execute("create table bar(i int, s string)");
		hiveShell.execute("create table baz(ai array<int>, d double)");
		hiveShell.execute("create table employee(id int,name string,dep string,salary int,age int)");
		hiveShell.execute("create table dest (x int)");
		hiveShell.execute("create table destp (x int) partitioned by (p string, q string)");
		hiveShell.execute("CREATE TABLE src (key STRING, value STRING)");
		hiveShell.execute("CREATE TABLE srcpart (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING)");
		hiveShell.insertInto("default", "foo").addRow(1, 1).addRow(2, 2).addRow(3, 3).addRow(4, 4).addRow(5, 5).commit();
		hiveShell.insertInto("default", "bar").addRow(1, "a").addRow(1, "aa").addRow(2, "b").commit();
		hiveShell.insertInto("default", "baz").addRow(Arrays.asList(1, 2, 3), 3.0).commit();
		hiveShell.insertInto("default", "src").addRow("1", "val1").addRow("2", "val2").addRow("3", "val3").commit();
		hiveShell.insertInto("default", "employee")
				.addRow(1, "A", "Management", 4500, 55)
				.addRow(2, "B", "Management", 4400, 61)
				.addRow(3, "C", "Management", 4000, 42)
				.addRow(4, "D", "Production", 3700, 35)
				.addRow(5, "E", "Production", 3500, 24)
				.addRow(6, "F", "Production", 3600, 28)
				.addRow(7, "G", "Production", 3800, 35)
				.addRow(8, "H", "Production", 4000, 52)
				.addRow(9, "I", "Service", 4100, 40)
				.addRow(10, "J", "Sales", 4300, 36)
				.addRow(11, "K", "Sales", 4100, 38)
				.commit();

		hiveShell.execute("create function hiveudf as 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd'");
		hiveShell.execute("create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");

		String[] queries = new String[]{
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
				"select hiveudf(x,y) from foo",
				"select hiveudtf(ai) from baz",
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
				"select col1,d from baz lateral view hiveudtf(ai) tbl1 as col1",
				"select col1,col2,d from baz lateral view hiveudtf(ai) tbl1 as col1 lateral view hiveudtf(ai) tbl2 as col2"
		};
		List<String> toRun = new ArrayList<>(Arrays.asList(queries));
		// add test cases specific to each version
		if (HiveVersionTestUtil.HIVE_220_OR_LATER) {
			toRun.add("select weekofyear(current_timestamp()), dayofweek(current_timestamp()) from src limit 1");
		}
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog(SqlDialect.HIVE);
//		runQuery("select col1,col2,d from baz lateral view hiveudtf(ai) tbl1 as col1 lateral view hiveudtf(ai) tbl2 as col2", tableEnv);
		for (String query : toRun) {
			runQuery(query, tableEnv);
		}
		System.out.println("finished");
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
}
