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

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hive QFile tests.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
@Ignore
public class HiveQFileITCase {

	private static final String START = "index_bitmap_auto.q";
	private static final String END = null;

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	private static HiveCatalog hiveCatalog;
	private static HiveMetastoreClientWrapper hmsClient;
	private static final String QTEST_DIR = Thread.currentThread().getContextClassLoader().getResource("qtest").getPath();
	private static final String QFILES_DIR = QTEST_DIR + "/queries/clientpositive";
	// map from conf name to its default value
	private static final Map<String, String> ALLOWED_SETTINGS =
			Stream.of(ConfVars.HIVE_QUOTEDID_SUPPORT, ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
					ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS)
					.collect(Collectors.toMap(HiveConf.ConfVars::toString, HiveConf.ConfVars::getDefaultValue));
	private static BufferedWriter fileWriter;

	static {
		ALLOWED_SETTINGS.put("parquet.column.index.access", "false");
		ALLOWED_SETTINGS.put(ConfVars.HIVEMAPREDMODE.varname, "nonstrict");
	}

	private boolean verbose = false;
	private Boolean useMRReader = false;

	@BeforeClass
	public static void setup() throws Exception {
		HiveConf hiveConf = hiveShell.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
		hmsClient = HiveMetastoreClientFactory.create(hiveConf, HiveShimLoader.getHiveVersion());
		init();
		// The default SerDe doesn't work for the tests, which is inline with Hive
		setConf(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE.varname, ColumnarSerDe.class.getCanonicalName());
		fileWriter = new BufferedWriter(new FileWriter(new File("target/qtest-result")));
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (fileWriter != null) {
			fileWriter.close();
		}
		if (hiveCatalog != null) {
			hiveCatalog.close();
		}
		if (hmsClient != null) {
			hmsClient.close();
		}
	}

	@Test
	public void runQTest() throws Exception {
		File[] qfiles = new File(QFILES_DIR).listFiles();
		int numFile = 0;
		int numStatement = 0;
		int numByFlink = 0;
		int numSuccessStatement = 0;
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog(true);
		boolean runThrough = true;
		boolean allTests = true;
		for (File qfile : qfiles) {
			if (!withinRange(qfile)) {
				continue;
			}
			numFile++;
			RunQFileResult result = runQFile(qfile, tableEnv, runThrough);
			numStatement += result.total();
			numByFlink += result.success + result.fail;
			numSuccessStatement += result.success;
			if (!allTests && !result.errors.isEmpty()) {
				break;
			}
		}
		println(String.format("Totally %d qfiles, %d attempted. Totally %d statements, %d run by Flink and %d succeeded",
				qfiles.length, numFile, numStatement, numByFlink, numSuccessStatement));
	}

	@Test
	public void runSingleQTest() throws Exception {
		File qfile = new File(QFILES_DIR, "index_bitmap.q");
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog(true);
		verbose = true;
		runQFile(qfile, tableEnv, true);
	}

	private static boolean withinRange(File qfile) {
		return (START == null || qfile.getName().compareTo(START) >= 0) && (END == null || qfile.getName().compareTo(END) <= 0);
	}

	private static void println(String s) throws Exception {
		if (s == null) {
			s = "null";
		}
		fileWriter.write(s);
		fileWriter.newLine();
		fileWriter.flush();
		System.out.println(s);
	}

	private static String[] extractStatements(File qfile) throws Exception {
		StringBuilder builder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(qfile))) {
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (!line.isEmpty() && !line.startsWith("--")) {
					if (builder.length() > 0) {
						builder.append(" ");
					}
					builder.append(line);
				}
				line = reader.readLine();
			}
		}
		if (builder.length() > 0 && builder.charAt(builder.length() - 1) == ';') {
			builder.deleteCharAt(builder.length() - 1);
		}
		return builder.toString().split(";\\s+");
	}

	private RunQFileResult runQFile(File qfile, TableEnvironment tableEnv, boolean runThrough) throws Exception {
		String[] statements = extractStatements(qfile);
		int success = 0;
		int fail = 0;
		int unsupported = 0;
		int skipped = 0;
		int delegated = 0;
		List<Throwable> errors = new ArrayList<>();
		println(String.format("*********Running %s with %d statements*********", qfile.getName(), statements.length));
		// reset to default database
		hiveShell.execute("use default");
		tableEnv.useDatabase("default");
		// reset to default settings
		for (Map.Entry<String, String> entry : ALLOWED_SETTINGS.entrySet()) {
			setConf(entry.getKey(), entry.getValue());
		}
		for (String statement : statements) {
			statement = statement.trim();
			if (statement.isEmpty() || statement.startsWith("--")) {
				continue;
			}
			statement = statement.replaceAll("\\s+", " ");
			// TODO: remove this once we can handle DDLs
			statement = fixTempTable(statement);
			boolean byFlink = false;
			try {
				String first = statement.split(" ")[0].toLowerCase();
				if (first.equals("explain") || first.equals("describe") || first.equals("show") ||
						first.equals("analyze") || first.equals("desc")) {
					skipped++;
				} else if (first.equals("set")) {
					String[] keyAndVal = statement.substring(4).replaceAll("\\s+", "").toLowerCase().split("=");
					if (ALLOWED_SETTINGS.containsKey(keyAndVal[0])) {
						setConf(keyAndVal[0], keyAndVal[1]);
						delegated++;
					} else {
						skipped++;
					}
				} else if (first.equals("use")) {
					hiveShell.execute(statement);
					String currentDB = hiveShell.executeQuery("select current_database()").get(0);
					tableEnv.useDatabase(currentDB);
					skipped++;
				} else if (first.equals("select")) {
					byFlink = true;
					runQuery(tableEnv, statement);
					success++;
				} else if (first.equals("insert")) {
					byFlink = true;
					runUpdate(tableEnv, statement);
					success++;
				} else if (needDelegate(first)) {
					delegated++;
					hiveShell.execute(statement);
				} else {
					unsupported++;
					throw new UnsupportedOperationException("Unsupported statement: " + statement);
				}
			} catch (Throwable t) {
				if (byFlink) {
					fail++;
					errors.add(t);
				}
				if (verbose && !(t instanceof UnsupportedOperationException)) {
					println("Failed to run statement: " + statement);
					println(ExceptionUtils.stringifyException(t));
				} else {
					println(t.getMessage());
				}
				if (!runThrough) {
					break;
				}
			}
		}
		RunQFileResult res = new RunQFileResult(success, fail, delegated, unsupported, skipped, errors);
		println(String.format("*********Finished %s: %s*********\n\n", qfile.getName(), res.toString()));
		return res;
	}

	// TODO: remove this once we can handle DDLs
	private String fixTempTable(String statement) {
		if (statement.toLowerCase().startsWith("create temporary table ")) {
			return "create table " + statement.substring("create temporary table ".length());
		}
		return statement;
	}

	private boolean needDelegate(String statement) {
		return statement.equals("create") || statement.equals("load") ||
				statement.equals("drop") || statement.equals("alter");
	}

	private static void setConf(String key, String val) {
		hiveShell.getHiveConf().set(key, val);
		hiveShell.execute(String.format("set %s=%s", key, val));
	}

	private static void init() {
		hiveShell.execute("set test.data.dir=" + QTEST_DIR + "/data");
		hiveShell.execute("DROP TABLE IF EXISTS primitives");
		hiveShell.execute("CREATE TABLE primitives (\n" +
				"                            id INT COMMENT 'default',\n" +
				"                            bool_col BOOLEAN COMMENT 'default',\n" +
				"                            tinyint_col TINYINT COMMENT 'default',\n" +
				"                            smallint_col SMALLINT COMMENT 'default',\n" +
				"                            int_col INT COMMENT 'default',\n" +
				"                            bigint_col BIGINT COMMENT 'default',\n" +
				"                            float_col FLOAT COMMENT 'default',\n" +
				"                            double_col DOUBLE COMMENT 'default',\n" +
				"                            date_string_col STRING COMMENT 'default',\n" +
				"                            string_col STRING COMMENT 'default',\n" +
				"                            timestamp_col TIMESTAMP COMMENT 'default')\n" +
				"    PARTITIONED BY (year INT COMMENT 'default', month INT COMMENT 'default')\n" +
				"ROW FORMAT DELIMITED\n" +
				"  FIELDS TERMINATED BY ','\n" +
				"  ESCAPED BY '\\\\'\n" +
				"STORED AS TEXTFILE");
		String initScript = "q_test_init.sql";
		hiveShell.execute(new File(QTEST_DIR, initScript));

		// seems these tables should be created by each qfile
		hiveShell.execute("drop table dest1");
		hiveShell.execute("drop table dest2");
	}

	private void runUpdate(TableEnvironment tableEnv, String dml) throws Exception {
		tableEnv.sqlUpdate(dml);
		tableEnv.execute("Run " + dml);
		if (verbose) {
			println("Successfully executed dml: " + dml);
		}
	}

	private void runQuery(TableEnvironment tableEnv, String query) throws Exception {
		Table table = tableEnv.sqlQuery(query);
		if (verbose) {
			println(table.explain());
		}
		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
		if (verbose) {
			println("Successfully executed query: " + query);
			println(results.toString());
		}
	}

	private TableEnvironment getTableEnvWithHiveCatalog(boolean hiveCompatible) {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(hiveCompatible ? SqlDialect.HIVE : SqlDialect.DEFAULT);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		// automatically load hive module in hive-compatible mode
		// TODO: move this logic to table env impl
		if (hiveCompatible) {
			HiveModule hiveModule = new HiveModule(hiveCatalog.getHiveVersion());
			CoreModule coreModule = CoreModule.INSTANCE;
			for (String loaded : tableEnv.listModules()) {
				tableEnv.unloadModule(loaded);
			}
			tableEnv.loadModule("hive", hiveModule);
			tableEnv.loadModule("core", coreModule);
		}
		tableEnv.getConfig().getConfiguration().setString(
				HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER.key(), useMRReader.toString());
		return tableEnv;
	}

	private static class RunQFileResult {
		private final int success;
		private final int fail;
		private final int delegated;
		private final int unsupported;
		private final int skipped;
		private final List<Throwable> errors;

		private RunQFileResult(int success, int fail, int delegated, int unsupported, int skipped, List<Throwable> errors) {
			this.success = success;
			this.fail = fail;
			this.delegated = delegated;
			this.unsupported = unsupported;
			this.skipped = skipped;
			this.errors = errors;
		}

		private int total() {
			return success + fail + delegated + unsupported + skipped;
		}

		@Override
		public String toString() {
			return String.format("Success %d, Fail %d, Delegated %d, Unsupported %d, Skipped %d",
					success, fail, delegated, unsupported, skipped);
		}
	}
}
