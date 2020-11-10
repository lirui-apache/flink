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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Shim for Hive version 2.0.0.
 */
public class HiveShimV200 extends HiveShimV122 {

	private static Method getAnalyzer;
	private static Method getHiveOperation;

	private static boolean inited = false;

	private static void init() {
		if (!inited) {
			synchronized (HiveShimV200.class) {
				if (!inited) {
					try {
						getAnalyzer = SemanticAnalyzerFactory.class.getDeclaredMethod("get", HiveConf.class, ASTNode.class);
						getAnalyzer.setAccessible(true);
						getHiveOperation = SessionState.class.getDeclaredMethod("getHiveOperation");
						getHiveOperation.setAccessible(true);
						inited = true;
					} catch (Exception e) {
						throw new FlinkHiveException("Failed to init shim methods", e);
					}
				}
			}
		}
	}

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			Class<?>[] constructorArgTypes = {HiveConf.class};
			Object[] constructorArgs = {hiveConf};
			Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class,
				constructorArgTypes.getClass(), constructorArgs.getClass(), String.class);
			// getProxy is a static method
			return (IMetaStoreClient) method.invoke(null, hiveConf, constructorArgTypes, constructorArgs,
				HiveMetaStoreClient.class.getName());
		} catch (Exception ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	public BulkWriter.Factory<RowData> createOrcBulkWriterFactory(
			Configuration conf, String schema, LogicalType[] fieldTypes) {
		return new OrcBulkWriterFactory<>(
				new RowDataVectorizer(schema, fieldTypes),
				new Properties(),
				conf);
	}

	@Override
	public Tuple2<BaseSemanticAnalyzer, HiveOperation> getAnalyzerAndOperation(ASTNode node, HiveConf hiveConf, Object queryState) {
		init();
		try {
			BaseSemanticAnalyzer analyzer = (BaseSemanticAnalyzer) getAnalyzer.invoke(null, hiveConf, node);
			HiveOperation operation = null;
			SessionState sessionState = SessionState.get();
			if (sessionState != null) {
				operation = (HiveOperation) getHiveOperation.invoke(sessionState);
			}
			return new Tuple2<>(analyzer, operation);
		} catch (Exception e) {
			throw new FlinkHiveException(e);
		}
	}
}
