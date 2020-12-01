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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.connectors.hive.FlinkHiveException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;

import java.lang.reflect.Method;

/**
 * Shim for Hive version 1.2.1.
 */
public class HiveShimV121 extends HiveShimV120 {

	private static Method getAnalyzer;

	private static boolean inited = false;

	private static void init() {
		if (!inited) {
			synchronized (HiveShimV200.class) {
				if (!inited) {
					try {
						getAnalyzer = SemanticAnalyzerFactory.class.getDeclaredMethod("get", HiveConf.class, ASTNode.class);
						getAnalyzer.setAccessible(true);
						inited = true;
					} catch (Exception e) {
						throw new FlinkHiveException("Failed to init shim methods", e);
					}
				}
			}
		}
	}

	@Override
	public BaseSemanticAnalyzer getAnalyzer(ASTNode node, HiveConf hiveConf, Object queryState) {
		init();
		try {
			return  (BaseSemanticAnalyzer) getAnalyzer.invoke(null, hiveConf, node);
		} catch (Exception e) {
			throw new FlinkHiveException(e);
		}
	}

}
