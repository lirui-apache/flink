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

package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains static methods of hive's BaseSemanticAnalyzer.
 */
public class HiveParserBaseSemanticAnalyzer {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParserBaseSemanticAnalyzer.class);

	private HiveParserBaseSemanticAnalyzer() {
	}

	/**
	 * Converts parsed key/value properties pairs into a map.
	 *
	 * @param prop ASTNode parent of the key/value pairs
	 *
	 * @param mapProp property map which receives the mappings
	 */
	public static void readProps(ASTNode prop, Map<String, String> mapProp) {

		for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
			String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
					.getText());
			String value = null;
			if (prop.getChild(propChild).getChild(1) != null) {
				value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
			}
			mapProp.put(key, value);
		}
	}

	public static String[] getQualifiedTableName(ASTNode tabNameNode) throws SemanticException {
		if (tabNameNode.getType() != HiveASTParser.TOK_TABNAME ||
				(tabNameNode.getChildCount() != 1 && tabNameNode.getChildCount() != 2)) {
			throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME.getMsg(tabNameNode));
		}
		if (tabNameNode.getChildCount() == 2) {
			String dbName = unescapeIdentifier(tabNameNode.getChild(0).getText());
			String tableName = unescapeIdentifier(tabNameNode.getChild(1).getText());
			return new String[] {dbName, tableName};
		}
		String tableName = unescapeIdentifier(tabNameNode.getChild(0).getText());
		return Utilities.getDbTableName(tableName);
	}

	public static String charSetString(String charSetName, String charSetString)
			throws SemanticException {
		try {
			// The character set name starts with a _, so strip that
			charSetName = charSetName.substring(1);
			if (charSetString.charAt(0) == '\'') {
				return new String(unescapeSQLString(charSetString).getBytes(),
						charSetName);
			} else {
				assert charSetString.charAt(0) == '0';
				assert charSetString.charAt(1) == 'x';
				charSetString = charSetString.substring(2);

				byte[] bArray = new byte[charSetString.length() / 2];
				int j = 0;
				for (int i = 0; i < charSetString.length(); i += 2) {
					int val = Character.digit(charSetString.charAt(i), 16) * 16
							+ Character.digit(charSetString.charAt(i + 1), 16);
					if (val > 127) {
						val = val - 256;
					}
					bArray[j++] = (byte) val;
				}

				String res = new String(bArray, charSetName);
				return res;
			}
		} catch (UnsupportedEncodingException e) {
			throw new SemanticException(e);
		}
	}

	public static String stripQuotes(String val) {
		return PlanUtils.stripQuotes(val);
	}

	/**
	 * Escapes the string for AST; doesn't enclose it in quotes, however.
	 */
	public static String escapeSQLString(String b) {
		// There's usually nothing to escape so we will be optimistic.
		String result = b;
		for (int i = 0; i < result.length(); ++i) {
			char currentChar = result.charAt(i);
			if (currentChar == '\\' && ((i + 1) < result.length())) {
				// TODO: do we need to handle the "this is what MySQL does" here?
				char nextChar = result.charAt(i + 1);
				if (nextChar == '%' || nextChar == '_') {
					++i;
					continue;
				}
			}
			switch (currentChar) {
				case '\0':
					result = spliceString(result, i, "\\0");
					++i;
					break;
				case '\'':
					result = spliceString(result, i, "\\'");
					++i;
					break;
				case '\"':
					result = spliceString(result, i, "\\\"");
					++i;
					break;
				case '\b':
					result = spliceString(result, i, "\\b");
					++i;
					break;
				case '\n':
					result = spliceString(result, i, "\\n");
					++i;
					break;
				case '\r':
					result = spliceString(result, i, "\\r");
					++i;
					break;
				case '\t':
					result = spliceString(result, i, "\\t");
					++i;
					break;
				case '\\':
					result = spliceString(result, i, "\\\\");
					++i;
					break;
				case '\u001A':
					result = spliceString(result, i, "\\Z");
					++i;
					break;
				default: {
					if (currentChar < ' ') {
						String hex = Integer.toHexString(currentChar);
						String unicode = "\\u";
						for (int j = 4; j > hex.length(); --j) {
							unicode += '0';
						}
						unicode += hex;
						result = spliceString(result, i, unicode);
						i += (unicode.length() - 1);
					}
					break; // if not a control character, do nothing
				}
			}
		}
		return result;
	}

	/**
	 * Remove the encapsulating "`" pair from the identifier. We allow users to
	 * use "`" to escape identifier for table names, column names and aliases, in
	 * case that coincide with Hive language keywords.
	 */
	public static String unescapeIdentifier(String val) {
		if (val == null) {
			return null;
		}
		if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}

	private static String spliceString(String str, int i, String replacement) {
		return spliceString(str, i, 1, replacement);
	}

	private static String spliceString(String str, int i, int length, String replacement) {
		return str.substring(0, i) + replacement + str.substring(i + length);
	}

	/**
	 * Get the unqualified name from a table node.
	 * This method works for table names qualified with their schema (e.g., "db.table")
	 * and table names without schema qualification. In both cases, it returns
	 * the table name without the schema.
	 *
	 * @param node the table node
	 * @return the table name without schema qualification
	 * (i.e., if name is "db.table" or "table", returns "table")
	 */
	public static String getUnescapedUnqualifiedTableName(ASTNode node) {
		assert node.getChildCount() <= 2;

		if (node.getChildCount() == 2) {
			node = (ASTNode) node.getChild(1);
		}

		return getUnescapedName(node);
	}

	/**
	 * Get dequoted name from a table/column node.
	 *
	 * @param tableOrColumnNode the table or column node
	 * @return for table node, db.tab or tab. for column node column.
	 */
	public static String getUnescapedName(ASTNode tableOrColumnNode) {
		return getUnescapedName(tableOrColumnNode, null);
	}

	public static String getUnescapedName(ASTNode tableOrColumnNode, String currentDatabase) {
		int tokenType = tableOrColumnNode.getToken().getType();
		if (tokenType == HiveASTParser.TOK_TABNAME) {
			// table node
			Map.Entry<String, String> dbTablePair = getDbTableNamePair(tableOrColumnNode);
			String dbName = dbTablePair.getKey();
			String tableName = dbTablePair.getValue();
			if (dbName != null) {
				return dbName + "." + tableName;
			}
			if (currentDatabase != null) {
				return currentDatabase + "." + tableName;
			}
			return tableName;
		} else if (tokenType == HiveASTParser.StringLiteral) {
			return unescapeSQLString(tableOrColumnNode.getText());
		}
		// column node
		return unescapeIdentifier(tableOrColumnNode.getText());
	}

	public static Map.Entry<String, String> getDbTableNamePair(ASTNode tableNameNode) {
		assert (tableNameNode.getToken().getType() == HiveASTParser.TOK_TABNAME);
		if (tableNameNode.getChildCount() == 2) {
			String dbName = unescapeIdentifier(tableNameNode.getChild(0).getText());
			String tableName = unescapeIdentifier(tableNameNode.getChild(1).getText());
			return Pair.of(dbName, tableName);
		} else {
			String tableName = unescapeIdentifier(tableNameNode.getChild(0).getText());
			return Pair.of(null, tableName);
		}
	}

	@SuppressWarnings("nls")
	public static String unescapeSQLString(String b) {
		Character enclosure = null;

		// Some of the strings can be passed in as unicode. For example, the
		// delimiter can be passed in as \002 - So, we first check if the
		// string is a unicode number, else go back to the old behavior
		StringBuilder sb = new StringBuilder(b.length());
		for (int i = 0; i < b.length(); i++) {

			char currentChar = b.charAt(i);
			if (enclosure == null) {
				if (currentChar == '\'' || b.charAt(i) == '\"') {
					enclosure = currentChar;
				}
				// ignore all other chars outside the enclosure
				continue;
			}

			if (enclosure.equals(currentChar)) {
				enclosure = null;
				continue;
			}

			if (currentChar == '\\' && (i + 6 < b.length()) && b.charAt(i + 1) == 'u') {
				int code = 0;
				int base = i + 2;
				for (int j = 0; j < 4; j++) {
					int digit = Character.digit(b.charAt(j + base), 16);
					code = (code << 4) + digit;
				}
				sb.append((char) code);
				i += 5;
				continue;
			}

			if (currentChar == '\\' && (i + 4 < b.length())) {
				char i1 = b.charAt(i + 1);
				char i2 = b.charAt(i + 2);
				char i3 = b.charAt(i + 3);
				if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
						&& (i3 >= '0' && i3 <= '7')) {
					byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
					byte[] bValArr = new byte[1];
					bValArr[0] = bVal;
					String tmp = new String(bValArr);
					sb.append(tmp);
					i += 3;
					continue;
				}
			}

			if (currentChar == '\\' && (i + 2 < b.length())) {
				char n = b.charAt(i + 1);
				switch (n) {
					case '0':
						sb.append("\0");
						break;
					case '\'':
						sb.append("'");
						break;
					case '"':
						sb.append("\"");
						break;
					case 'b':
						sb.append("\b");
						break;
					case 'n':
						sb.append("\n");
						break;
					case 'r':
						sb.append("\r");
						break;
					case 't':
						sb.append("\t");
						break;
					case 'Z':
						sb.append("\u001A");
						break;
					case '\\':
						sb.append("\\");
						break;
					// The following 2 lines are exactly what MySQL does TODO: why do we do this?
					case '%':
						sb.append("\\%");
						break;
					case '_':
						sb.append("\\_");
						break;
					default:
						sb.append(n);
				}
				i++;
			} else {
				sb.append(currentChar);
			}
		}
		return sb.toString();
	}

	public static void validatePartSpec(Table tbl, Map<String, String> partSpec,
			ASTNode astNode, HiveConf conf, boolean shouldBeFull) throws SemanticException {
		tbl.validatePartColumnNames(partSpec, shouldBeFull);
		validatePartColumnType(tbl, partSpec, astNode, conf);
	}

	private static boolean getPartExprNodeDesc(ASTNode astNode, HiveConf conf,
			Map<ASTNode, ExprNodeDesc> astExprNodeMap) throws SemanticException {

		if (astNode == null) {
			return true;
		} else if ((astNode.getChildren() == null) || (astNode.getChildren().size() == 0)) {
			return astNode.getType() != HiveASTParser.TOK_PARTVAL;
		}

		HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(null);
		String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
		boolean result = true;
		for (Node childNode : astNode.getChildren()) {
			ASTNode childASTNode = (ASTNode) childNode;

			if (childASTNode.getType() != HiveASTParser.TOK_PARTVAL) {
				result = getPartExprNodeDesc(childASTNode, conf, astExprNodeMap) && result;
			} else {
				boolean isDynamicPart = childASTNode.getChildren().size() <= 1;
				result = !isDynamicPart && result;
				if (!isDynamicPart) {
					ASTNode partVal = (ASTNode) childASTNode.getChildren().get(1);
					if (!defaultPartitionName.equalsIgnoreCase(unescapeSQLString(partVal.getText()))) {
						astExprNodeMap.put((ASTNode) childASTNode.getChildren().get(0),
								HiveParserTypeCheckProcFactory.genExprNode(partVal, typeCheckCtx).get(partVal));
					}
				}
			}
		}
		return result;
	}

	public static String stripIdentifierQuotes(String val) {
		if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}

	public static void validatePartColumnType(Table tbl, Map<String, String> partSpec,
			ASTNode astNode, HiveConf conf) throws SemanticException {
		if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TYPE_CHECK_ON_INSERT)) {
			return;
		}

		Map<ASTNode, ExprNodeDesc> astExprNodeMap = new HashMap<ASTNode, ExprNodeDesc>();
		if (!getPartExprNodeDesc(astNode, conf, astExprNodeMap)) {
			LOG.warn("Dynamic partitioning is used; only validating "
					+ astExprNodeMap.size() + " columns");
		}

		if (astExprNodeMap.isEmpty()) {
			return; // All columns are dynamic, nothing to do.
		}

		List<FieldSchema> parts = tbl.getPartitionKeys();
		Map<String, String> partCols = new HashMap<String, String>(parts.size());
		for (FieldSchema col : parts) {
			partCols.put(col.getName(), col.getType().toLowerCase());
		}
		for (Map.Entry<ASTNode, ExprNodeDesc> astExprNodePair : astExprNodeMap.entrySet()) {
			String astKeyName = astExprNodePair.getKey().toString().toLowerCase();
			if (astExprNodePair.getKey().getType() == HiveASTParser.Identifier) {
				astKeyName = stripIdentifierQuotes(astKeyName);
			}
			String colType = partCols.get(astKeyName);
			ObjectInspector inputOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo
					(astExprNodePair.getValue().getTypeInfo());

			TypeInfo expectedType =
					TypeInfoUtils.getTypeInfoFromTypeString(colType);
			ObjectInspector outputOI =
					TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
			//  Since partVal is a constant, it is safe to cast ExprNodeDesc to ExprNodeConstantDesc.
			//  Its value should be in normalized format (e.g. no leading zero in integer, date is in
			//  format of YYYY-MM-DD etc)
			Object value = ((ExprNodeConstantDesc) astExprNodePair.getValue()).getValue();
			Object convertedValue = value;
			if (!inputOI.getTypeName().equals(outputOI.getTypeName())) {
				convertedValue = ObjectInspectorConverters.getConverter(inputOI, outputOI).convert(value);
				if (convertedValue == null) {
					throw new SemanticException(ErrorMsg.PARTITION_SPEC_TYPE_MISMATCH, astKeyName,
							inputOI.getTypeName(), outputOI.getTypeName());
				}

				if (!convertedValue.toString().equals(value.toString())) {
					//  value might have been changed because of the normalization in conversion
					LOG.warn("Partition " + astKeyName + " expects type " + outputOI.getTypeName()
							+ " but input value is in type " + inputOI.getTypeName() + ". Convert "
							+ value.toString() + " to " + convertedValue.toString());
				}
			}

			if (!convertedValue.toString().equals(partSpec.get(astKeyName))) {
				LOG.warn("Partition Spec " + astKeyName + "=" + partSpec.get(astKeyName)
						+ " has been changed to " + astKeyName + "=" + convertedValue.toString());
			}
			partSpec.put(astKeyName, convertedValue.toString());
		}
	}

	private static void errorPartSpec(Map<String, String> partSpec,
			List<FieldSchema> parts) throws SemanticException {
		StringBuilder sb =
				new StringBuilder(
						"Partition columns in the table schema are: (");
		for (FieldSchema fs : parts) {
			sb.append(fs.getName()).append(", ");
		}
		sb.setLength(sb.length() - 2); // remove the last ", "
		sb.append("), while the partitions specified in the query are: (");

		Iterator<String> itrPsKeys = partSpec.keySet().iterator();
		while (itrPsKeys.hasNext()) {
			sb.append(itrPsKeys.next()).append(", ");
		}
		sb.setLength(sb.length() - 2); // remove the last ", "
		sb.append(").");
		throw new SemanticException(ErrorMsg.PARTSPEC_DIFFER_FROM_SCHEMA
				.getMsg(sb.toString()));
	}

	/**
	 * TableSpec.
	 */
	public static class TableSpec {
		public String tableName;
		public Table tableHandle;
		public Map<String, String> partSpec; // has to use LinkedHashMap to enforce order
		public Partition partHandle;
		public int numDynParts; // number of dynamic partition columns
		public List<Partition> partitions; // involved partitions in TableScanOperator/FileSinkOperator

		/**
		 * SpecType.
		 */
		public enum SpecType {TABLE_ONLY, STATIC_PARTITION, DYNAMIC_PARTITION}

		public TableSpec.SpecType specType;

		public TableSpec(Hive db, HiveConf conf, ASTNode ast)
				throws SemanticException {
			this(db, conf, ast, true, false);
		}

		public TableSpec(Hive db, HiveConf conf, String tableName, Map<String, String> partSpec)
				throws HiveException {
			this.tableName = tableName;
			this.partSpec = partSpec;
			this.tableHandle = db.getTable(tableName);
			if (partSpec != null) {
				this.specType = TableSpec.SpecType.STATIC_PARTITION;
				this.partHandle = db.getPartition(tableHandle, partSpec, false);
				this.partitions = Arrays.asList(partHandle);
			} else {
				this.specType = TableSpec.SpecType.TABLE_ONLY;
			}
		}

		public TableSpec(Table tableHandle, List<Partition> partitions)
				throws HiveException {
			this.tableHandle = tableHandle;
			this.tableName = tableHandle.getTableName();
			if (partitions != null && !partitions.isEmpty()) {
				this.specType = TableSpec.SpecType.STATIC_PARTITION;
				this.partitions = partitions;
				List<FieldSchema> partCols = this.tableHandle.getPartCols();
				this.partSpec = new LinkedHashMap<>();
				for (FieldSchema partCol : partCols) {
					partSpec.put(partCol.getName(), null);
				}
			} else {
				this.specType = TableSpec.SpecType.TABLE_ONLY;
			}
		}

		public TableSpec(Hive db, HiveConf conf, ASTNode ast, boolean allowDynamicPartitionsSpec,
				boolean allowPartialPartitionsSpec) throws SemanticException {
			assert (ast.getToken().getType() == HiveASTParser.TOK_TAB
					|| ast.getToken().getType() == HiveASTParser.TOK_TABLE_PARTITION
					|| ast.getToken().getType() == HiveASTParser.TOK_TABTYPE
					|| ast.getToken().getType() == HiveASTParser.TOK_CREATETABLE
					|| ast.getToken().getType() == HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW);
			int childIndex = 0;
			numDynParts = 0;

			try {
				// get table metadata
				tableName = getUnescapedName((ASTNode) ast.getChild(0));
				boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
				if (testMode) {
					tableName = conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX)
							+ tableName;
				}
				if (ast.getToken().getType() != HiveASTParser.TOK_CREATETABLE &&
						ast.getToken().getType() != HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW) {
					tableHandle = db.getTable(tableName);
				}
			} catch (InvalidTableException ite) {
				throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast
						.getChild(0)), ite);
			} catch (HiveException e) {
				throw new SemanticException("Error while retrieving table metadata", e);
			}

			// get partition metadata if partition specified
			if (ast.getChildCount() == 2 && ast.getToken().getType() != HiveASTParser.TOK_CREATETABLE &&
					ast.getToken().getType() != HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW) {
				childIndex = 1;
				ASTNode partspec = (ASTNode) ast.getChild(1);
				partitions = new ArrayList<Partition>();
				// partSpec is a mapping from partition column name to its value.
				Map<String, String> tmpPartSpec = new HashMap<>(partspec.getChildCount());
				for (int i = 0; i < partspec.getChildCount(); ++i) {
					ASTNode partspecVal = (ASTNode) partspec.getChild(i);
					String val = null;
					String colName = unescapeIdentifier(partspecVal.getChild(0).getText().toLowerCase());
					if (partspecVal.getChildCount() < 2) { // DP in the form of T partition (ds, hr)
						if (allowDynamicPartitionsSpec) {
							++numDynParts;
						} else {
							throw new SemanticException(ErrorMsg.INVALID_PARTITION
									.getMsg(" - Dynamic partitions not allowed"));
						}
					} else { // in the form of T partition (ds="2010-03-03")
						val = stripQuotes(partspecVal.getChild(1).getText());
					}
					tmpPartSpec.put(colName, val);
				}

				// check if the columns, as well as value types in the partition() clause are valid
				validatePartSpec(tableHandle, tmpPartSpec, ast, conf, false);

				List<FieldSchema> parts = tableHandle.getPartitionKeys();
				partSpec = new LinkedHashMap<String, String>(partspec.getChildCount());
				for (FieldSchema fs : parts) {
					String partKey = fs.getName();
					partSpec.put(partKey, tmpPartSpec.get(partKey));
				}

				// check if the partition spec is valid
				if (numDynParts > 0) {
					int numStaPart = parts.size() - numDynParts;
					if (numStaPart == 0 &&
							conf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
						throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg());
					}

					// check the partitions in partSpec be the same as defined in table schema
					if (partSpec.keySet().size() != parts.size()) {
						errorPartSpec(partSpec, parts);
					}
					Iterator<String> itrPsKeys = partSpec.keySet().iterator();
					for (FieldSchema fs : parts) {
						if (!itrPsKeys.next().toLowerCase().equals(fs.getName().toLowerCase())) {
							errorPartSpec(partSpec, parts);
						}
					}

					// check if static partition appear after dynamic partitions
					for (FieldSchema fs : parts) {
						if (partSpec.get(fs.getName().toLowerCase()) == null) {
							if (numStaPart > 0) { // found a DP, but there exists ST as subpartition
								throw new SemanticException(
										ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg(ast.getChild(childIndex)));
							}
							break;
						} else {
							--numStaPart;
						}
					}
					partHandle = null;
					specType = TableSpec.SpecType.DYNAMIC_PARTITION;
				} else {
					try {
						if (allowPartialPartitionsSpec) {
							partitions = db.getPartitions(tableHandle, partSpec);
						} else {
							// this doesn't create partition.
							partHandle = db.getPartition(tableHandle, partSpec, false);
							if (partHandle == null) {
								// if partSpec doesn't exists in DB, return a delegate one
								// and the actual partition is created in MoveTask
								partHandle = new Partition(tableHandle, partSpec, null);
							} else {
								partitions.add(partHandle);
							}
						}
					} catch (HiveException e) {
						throw new SemanticException(
								ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(childIndex)), e);
					}
					specType = TableSpec.SpecType.STATIC_PARTITION;
				}
			} else {
				specType = TableSpec.SpecType.TABLE_ONLY;
			}
		}

		public Map<String, String> getPartSpec() {
			return this.partSpec;
		}

		public void setPartSpec(Map<String, String> partSpec) {
			this.partSpec = partSpec;
		}

		@Override
		public String toString() {
			if (partHandle != null) {
				return partHandle.toString();
			} else {
				return tableHandle.toString();
			}
		}
	}

	/**
	 * AnalyzeRewriteContext.
	 */
	public class AnalyzeRewriteContext {

		private String tableName;
		private List<String> colName;
		private List<String> colType;
		private boolean tblLvl;

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public List<String> getColName() {
			return colName;
		}

		public void setColName(List<String> colName) {
			this.colName = colName;
		}

		public boolean isTblLvl() {
			return tblLvl;
		}

		public void setTblLvl(boolean isTblLvl) {
			this.tblLvl = isTblLvl;
		}

		public List<String> getColType() {
			return colType;
		}

		public void setColType(List<String> colType) {
			this.colType = colType;
		}

	}
}
