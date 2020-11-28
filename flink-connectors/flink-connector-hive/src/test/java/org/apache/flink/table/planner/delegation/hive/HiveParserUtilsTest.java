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

package org.apache.flink.table.planner.delegation.hive;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for HiveParserUtils.
 */
public class HiveParserUtilsTest {

	@Test
	public void testSplitSQLStatements() {
		List<String> commands = HiveParserUtils.splitSQLStatements("show tables;select ';';\n -- comment1; comment2\n");
		assertEquals(Arrays.asList("show tables", "select ';'"), commands);
		commands = HiveParserUtils.splitSQLStatements("create table foo(\nx int --comment;\n);drop table foo;");
		assertEquals(Arrays.asList("create table foo(\nx int --comment;\n)", "drop table foo"), commands);
		commands = HiveParserUtils.splitSQLStatements("select 'a\\;\nb'\n;\nselect `a\\;b`,\"\\\"cd\";");
		assertEquals(Arrays.asList("select 'a\\;\nb'", "select `a\\;b`,\"\\\"cd\""), commands);
		commands = HiveParserUtils.splitSQLStatements("select '--ab;',--c;d'\n,ef");
		assertEquals(Arrays.asList("select '--ab;',--c;d'\n,ef"), commands);
	}
}
