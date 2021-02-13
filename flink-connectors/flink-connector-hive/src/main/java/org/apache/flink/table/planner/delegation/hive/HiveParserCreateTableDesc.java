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

import org.apache.flink.table.planner.delegation.hive.parse.HiveParserRowFormatParams;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserStorageFormat;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Desc for CREATE TABLE operation.
 */
public class HiveParserCreateTableDesc implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String compoundName;
	private final boolean isExternal;
	private final boolean ifNotExists;
	private final boolean isTemporary;
	private final List<FieldSchema> cols;
	private final List<FieldSchema> partCols;
	private final String comment;
	private final String location;
	private final Map<String, String> tblProps;
	private final HiveParserRowFormatParams rowFormatParams;
	private final HiveParserStorageFormat storageFormat;
	private final List<PrimaryKey> primaryKeys;

	public HiveParserCreateTableDesc(String compoundName, boolean isExternal, boolean ifNotExists, boolean isTemporary,
			List<FieldSchema> cols, List<FieldSchema> partCols, String comment, String location, Map<String, String> tblProps,
			HiveParserRowFormatParams rowFormatParams, HiveParserStorageFormat storageFormat, List<PrimaryKey> primaryKeys) {
		this.compoundName = compoundName;
		this.isExternal = isExternal;
		this.ifNotExists = ifNotExists;
		this.isTemporary = isTemporary;
		this.cols = cols;
		this.partCols = partCols;
		this.comment = comment;
		this.location = location;
		this.tblProps = tblProps;
		this.rowFormatParams = rowFormatParams;
		this.storageFormat = storageFormat;
		this.primaryKeys = primaryKeys;
	}

	public String getCompoundName() {
		return compoundName;
	}

	public boolean isExternal() {
		return isExternal;
	}

	public boolean ifNotExists() {
		return ifNotExists;
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	public List<FieldSchema> getCols() {
		return cols;
	}

	public List<FieldSchema> getPartCols() {
		return partCols;
	}

	public String getComment() {
		return comment;
	}

	public String getLocation() {
		return location;
	}

	public Map<String, String> getTblProps() {
		return tblProps;
	}

	public HiveParserRowFormatParams getRowFormatParams() {
		return rowFormatParams;
	}

	public HiveParserStorageFormat getStorageFormat() {
		return storageFormat;
	}

	public List<PrimaryKey> getPrimaryKeys() {
		return primaryKeys;
	}

	/**
	 * Counterpart of hive's SQLPrimaryKey.
	 */
	public static class PrimaryKey implements Serializable{

		private static final long serialVersionUID = 3036210046732750293L;

		private final String dbName;
		private final String tblName;
		private final String pk;
		private final String constraintName;
		private final boolean enable;
		private final boolean validate;
		private final boolean rely;

		public PrimaryKey(String dbName, String tblName, String pk, String constraintName, boolean enable, boolean validate, boolean rely) {
			this.dbName = dbName;
			this.tblName = tblName;
			this.pk = pk;
			this.constraintName = constraintName;
			this.enable = enable;
			this.validate = validate;
			this.rely = rely;
		}

		public String getDbName() {
			return dbName;
		}

		public String getTblName() {
			return tblName;
		}

		public String getPk() {
			return pk;
		}

		public String getConstraintName() {
			return constraintName;
		}

		public boolean isEnable() {
			return enable;
		}

		public boolean isValidate() {
			return validate;
		}

		public boolean isRely() {
			return rely;
		}
	}
}
