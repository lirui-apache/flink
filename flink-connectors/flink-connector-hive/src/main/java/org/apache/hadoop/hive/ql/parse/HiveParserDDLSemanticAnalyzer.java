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

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.delegation.hive.CTASDesc;
import org.apache.flink.table.planner.delegation.hive.DropPartitionDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserAlterTableDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserAuthorizationParseUtils;
import org.apache.flink.table.planner.delegation.hive.HiveParserCreateTableDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserCreateTableDesc.PrimaryKey;
import org.apache.flink.table.planner.delegation.hive.HiveParserCreateViewDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserDropFunctionDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserDropTableDesc;
import org.apache.flink.table.planner.delegation.hive.HiveParserShowTablesDesc;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.HiveParserContext;
import org.apache.hadoop.hive.ql.HiveParserQueryState;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableAlterPartDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.ShowCreateTableDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTblPropertiesDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getColumns;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getDotName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getQualifiedTableName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getTypeStringFromAST;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getUnescapedName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.readProps;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.stripQuotes;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeSQLString;

/**
 * Counterpart of hive's DDLSemanticAnalyzer, but also incorporated functionalities from SemanticAnalyzer and FunctionSemanticAnalyzer.
 */
public class HiveParserDDLSemanticAnalyzer {
	private static final Logger LOG = LoggerFactory.getLogger(HiveParserDDLSemanticAnalyzer.class);
	private static final Map<Integer, String> TokenToTypeName = new HashMap<>();
	private static final String MATERIALIZATION_MARKER = "$MATERIALIZATION";

	private final Set<String> reservedPartitionValues;
	private final HiveConf conf;
	private final HiveParserQueryState queryState;
	private final HiveParserContext ctx;
	private final HiveCatalog hiveCatalog;
	private final String currentDB;

	static {
		TokenToTypeName.put(HiveASTParser.TOK_BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_TINYINT, serdeConstants.TINYINT_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_INT, serdeConstants.INT_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_BIGINT, serdeConstants.BIGINT_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_FLOAT, serdeConstants.FLOAT_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_BINARY, serdeConstants.BINARY_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_DATETIME, serdeConstants.DATETIME_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_INTERVAL_YEAR_MONTH, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_INTERVAL_DAY_TIME, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
		TokenToTypeName.put(HiveASTParser.TOK_DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
	}

	public static String getTypeName(ASTNode node) throws SemanticException {
		int token = node.getType();
		String typeName;

		// datetime type isn't currently supported
		if (token == HiveASTParser.TOK_DATETIME) {
			throw new SemanticException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
		}

		switch (token) {
			case HiveASTParser.TOK_CHAR:
				CharTypeInfo charTypeInfo = ParseUtils.getCharTypeInfo(node);
				typeName = charTypeInfo.getQualifiedName();
				break;
			case HiveASTParser.TOK_VARCHAR:
				VarcharTypeInfo varcharTypeInfo = ParseUtils.getVarcharTypeInfo(node);
				typeName = varcharTypeInfo.getQualifiedName();
				break;
			case HiveASTParser.TOK_DECIMAL:
				DecimalTypeInfo decTypeInfo = ParseUtils.getDecimalTypeTypeInfo(node);
				typeName = decTypeInfo.getQualifiedName();
				break;
			default:
				typeName = TokenToTypeName.get(token);
		}
		return typeName;
	}

	public HiveParserDDLSemanticAnalyzer(HiveParserQueryState queryState, HiveParserContext ctx, HiveCatalog hiveCatalog, String currentDB) throws SemanticException {
		this.queryState = queryState;
		this.conf = queryState.getConf();
		this.ctx = ctx;
		this.hiveCatalog = hiveCatalog;
		this.currentDB = currentDB;
		reservedPartitionValues = new HashSet<>();
		// Partition can't have this name
		reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
		reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
		// Partition value can't end in this suffix
		reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ORIGINAL));
		reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ARCHIVED));
		reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_EXTRACTED));
	}

	private Table getTable(String tableName) throws SemanticException {
		return getTable(toObjectPath(tableName));
	}

	private Table getTable(ObjectPath tablePath) throws SemanticException {
		try {
			return new Table(hiveCatalog.getHiveTable(tablePath));
		} catch (TableNotExistException e) {
			throw new SemanticException(e);
		}
	}

	private ObjectPath toObjectPath(String name) throws SemanticException {
		String[] parts = Utilities.getDbTableName(currentDB, name);
		return new ObjectPath(parts[0], parts[1]);
	}

	private HashSet<ReadEntity> getInputs() {
		return new HashSet<>();
	}

	private HashSet<WriteEntity> getOutputs() {
		return new HashSet<>();
	}

	public Serializable analyzeInternal(ASTNode input) throws SemanticException {

		ASTNode ast = input;
		Serializable res = null;
		switch (ast.getType()) {
			case HiveASTParser.TOK_ALTERTABLE: {
				ast = (ASTNode) input.getChild(1);
				String[] qualified = getQualifiedTableName((ASTNode) input.getChild(0));
				String tableName = getDotName(qualified);
				HashMap<String, String> partSpec = null;
				ASTNode partSpecNode = (ASTNode) input.getChild(2);
				if (partSpecNode != null) {
					//  We can use alter table partition rename to convert/normalize the legacy partition
					//  column values. In so, we should not enable the validation to the old partition spec
					//  passed in this command.
					if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_RENAMEPART) {
						partSpec = getPartSpec(partSpecNode);
					} else {
						partSpec = getValidatedPartSpec(getTable(tableName), partSpecNode, conf, false);
					}
				}

				if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_RENAME) {
					res = analyzeAlterTableRename(qualified, ast, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_TOUCH) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ARCHIVE) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_UNARCHIVE) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ADDCOLS) {
					res = analyzeAlterTableModifyCols(qualified, ast, partSpec, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_REPLACECOLS) {
					res = analyzeAlterTableModifyCols(qualified, ast, partSpec, true);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_RENAMECOL) {
					res = analyzeAlterTableRenameCol(qualified, ast, partSpec);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ADDPARTS) {
					res = analyzeAlterTableAddParts(qualified, ast, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_DROPPARTS) {
					res = analyzeAlterTableDropParts(qualified, ast, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_PARTCOLTYPE) {
					res = analyzeAlterTablePartColType(qualified, ast);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_PROPERTIES) {
					res = analyzeAlterTableProps(qualified, null, ast, false, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_DROPPROPERTIES) {
					res = analyzeAlterTableProps(qualified, null, ast, false, true);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_UPDATESTATS) {
					res = analyzeAlterTableProps(qualified, partSpec, ast, false, false);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_SKEWED) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_EXCHANGEPARTITION) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_FILEFORMAT) {
					res = analyzeAlterTableFileFormat(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_LOCATION) {
					res = analyzeAlterTableLocation(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_MERGEFILES) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_SERIALIZER) {
					res = analyzeAlterTableSerde(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_SERDEPROPERTIES) {
					res = analyzeAlterTableSerdeProps(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_RENAMEPART) {
					res = analyzeAlterTableRenamePart(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_SKEWED_LOCATION) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_BUCKETS) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_CLUSTER_SORT) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_COMPACT) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_UPDATECOLSTATS) {
					res = analyzeAlterTableUpdateStats(ast, tableName, partSpec);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_DROPCONSTRAINT) {
					throw new SemanticException("Unsupported command: " + ast);
				} else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_ADDCONSTRAINT) {
					throw new SemanticException("Unsupported command: " + ast);
				}
				break;
			}
			case HiveASTParser.TOK_DROPTABLE:
				res = analyzeDropTable(ast, null);
				break;
			case HiveASTParser.TOK_DESCTABLE:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeDescribeTable(ast);
				break;
			case HiveASTParser.TOK_SHOWDATABASES:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowDatabases(ast);
				break;
			case HiveASTParser.TOK_SHOWTABLES:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowTables(ast);
				break;
			case HiveASTParser.TOK_SHOWCOLUMNS:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowColumns(ast);
				break;
			case HiveASTParser.TOK_SHOW_TABLESTATUS:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowTableStatus(ast);
				break;
			case HiveASTParser.TOK_SHOW_TBLPROPERTIES:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowTableProperties(ast);
				break;
			case HiveASTParser.TOK_SHOWFUNCTIONS:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowFunctions(ast);
				break;
			case HiveASTParser.TOK_SHOWCONF:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowConf(ast);
				break;
			case HiveASTParser.TOK_SHOWVIEWS:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowViews(ast);
				break;
			case HiveASTParser.TOK_DESCFUNCTION:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeDescFunction(ast);
				break;
			case HiveASTParser.TOK_DESCDATABASE:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeDescDatabase(ast);
				break;
			case HiveASTParser.TOK_DROPVIEW:
				res = analyzeDropTable(ast, TableType.VIRTUAL_VIEW);
				break;
			case HiveASTParser.TOK_ALTERVIEW: {
				if (ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
					// alter view as
					res = analyzeCreateView(ast);
				} else {
					String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
					ast = (ASTNode) ast.getChild(1);
					if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_PROPERTIES) {
						res = analyzeAlterTableProps(qualified, null, ast, true, false);
					} else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_DROPPROPERTIES) {
						res = analyzeAlterTableProps(qualified, null, ast, true, true);
					} else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_ADDPARTS) {
						res = analyzeAlterTableAddParts(qualified, ast, true);
					} else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_DROPPARTS) {
						res = analyzeAlterTableDropParts(qualified, ast, true);
					} else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_RENAME) {
						res = analyzeAlterTableRename(qualified, ast, true);
					}
				}
				break;
			}
			case HiveASTParser.TOK_SHOWPARTITIONS:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowPartitions(ast);
				break;
			case HiveASTParser.TOK_SHOW_CREATETABLE:
				ctx.setResFile(ctx.getLocalTmpPath());
				res = analyzeShowCreateTable(ast);
				break;
			case HiveASTParser.TOK_CREATEDATABASE:
				res = analyzeCreateDatabase(ast);
				break;
			case HiveASTParser.TOK_DROPDATABASE:
				res = analyzeDropDatabase(ast);
				break;
			case HiveASTParser.TOK_SWITCHDATABASE:
				res = analyzeSwitchDatabase(ast);
				break;
			case HiveASTParser.TOK_ALTERDATABASE_PROPERTIES:
				res = analyzeAlterDatabaseProperties(ast);
				break;
			case HiveASTParser.TOK_ALTERDATABASE_OWNER:
				res = analyzeAlterDatabaseOwner(ast);
				break;
			case HiveASTParser.TOK_CREATETABLE:
				res = analyzeCreateTable(ast);
				break;
			case HiveASTParser.TOK_CREATEVIEW:
				res = analyzeCreateView(ast);
				break;
			case HiveASTParser.TOK_CREATEFUNCTION:
				res = analyzerCreateFunction(ast);
				break;
			case HiveASTParser.TOK_DROPFUNCTION:
				res = analyzeDropFunction(ast);
				break;
			case HiveASTParser.TOK_TRUNCATETABLE:
			case HiveASTParser.TOK_CREATEINDEX:
			case HiveASTParser.TOK_DROPINDEX:
			case HiveASTParser.TOK_SHOWLOCKS:
			case HiveASTParser.TOK_SHOWDBLOCKS:
			case HiveASTParser.TOK_SHOW_COMPACTIONS:
			case HiveASTParser.TOK_SHOW_TRANSACTIONS:
			case HiveASTParser.TOK_ABORT_TRANSACTIONS:
			case HiveASTParser.TOK_MSCK:
			case HiveASTParser.TOK_ALTERINDEX_REBUILD:
			case HiveASTParser.TOK_ALTERINDEX_PROPERTIES:
			case HiveASTParser.TOK_SHOWINDEXES:
			case HiveASTParser.TOK_LOCKTABLE:
			case HiveASTParser.TOK_UNLOCKTABLE:
			case HiveASTParser.TOK_LOCKDB:
			case HiveASTParser.TOK_UNLOCKDB:
			case HiveASTParser.TOK_CREATEROLE:
			case HiveASTParser.TOK_DROPROLE:
			case HiveASTParser.TOK_SHOW_ROLE_GRANT:
			case HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS:
			case HiveASTParser.TOK_SHOW_ROLES:
			case HiveASTParser.TOK_GRANT_ROLE:
			case HiveASTParser.TOK_REVOKE_ROLE:
			case HiveASTParser.TOK_GRANT:
			case HiveASTParser.TOK_SHOW_GRANT:
			case HiveASTParser.TOK_REVOKE:
			case HiveASTParser.TOK_SHOW_SET_ROLE:
			case HiveASTParser.TOK_CACHE_METADATA:
			case HiveASTParser.TOK_DROP_MATERIALIZED_VIEW:
			case HiveASTParser.TOK_SHOW_CREATEDATABASE:
			default:
				throw new SemanticException("Unsupported command: " + ast);
		}
		return res;
	}

	private Serializable analyzeDropFunction(ASTNode ast) throws SemanticException {
		// ^(TOK_DROPFUNCTION identifier ifExists? $temp?)
		String functionName = ast.getChild(0).getText();
		boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);
		// we want to signal an error if the function doesn't exist and we're
		// configured not to ignore this
//		boolean isExists = ifExists || HiveConf.getBoolVar(conf, HiveConf.ConfVars.DROPIGNORESNONEXISTENT);
//		boolean throwException =
//				!ifExists && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.DROPIGNORESNONEXISTENT);

//		FunctionInfo info = FunctionRegistry.getFunctionInfo(functionName);
//		if (info == null) {
//			if (throwException) {
//				throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
//			} else {
//				// Fail silently
//				return;
//			}
//		} else if (info.isBuiltIn()) {
//			throw new SemanticException(ErrorMsg.DROP_NATIVE_FUNCTION.getMsg(functionName));
//		}

		boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
		DropFunctionDesc desc = new DropFunctionDesc(functionName, isTemporaryFunction);
		return new HiveParserDropFunctionDesc(desc, ifExists);
	}

	private Serializable analyzerCreateFunction(ASTNode ast) throws SemanticException {
		// ^(TOK_CREATEFUNCTION identifier StringLiteral ({isTempFunction}? => TOK_TEMPORARY))
		String functionName = ast.getChild(0).getText().toLowerCase();
		boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
		String className = unescapeSQLString(ast.getChild(1).getText());

		// Temp functions are not allowed to have qualified names.
		if (isTemporaryFunction && FunctionUtils.isQualifiedFunctionName(functionName)) {
			throw new SemanticException("Temporary function cannot be created with a qualified name.");
		}

		// find any referenced resources
//		List<ResourceUri> resources = getResourceList(ast);

		CreateFunctionDesc desc = new CreateFunctionDesc(functionName, isTemporaryFunction, className, Collections.emptyList());
		return new FunctionWork(desc);
	}

	private Serializable analyzeCreateView(ASTNode ast) throws SemanticException {
		String[] qualTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
		String dbDotTable = getDotName(qualTabName);
		List<FieldSchema> cols = null;
		boolean ifNotExists = false;
		boolean rewriteEnabled = false;
		boolean orReplace = false;
		boolean isAlterViewAs = false;
		String comment = null;
		ASTNode selectStmt = null;
		Map<String, String> tblProps = null;
		List<String> partColNames = null;
		boolean isMaterialized = ast.getToken().getType() == HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW;
		String location = null;
		HiveParserRowFormatParams rowFormatParams = new HiveParserRowFormatParams();
		HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

		LOG.info("Creating view " + dbDotTable + " position=" + ast.getCharPositionInLine());
		int numCh = ast.getChildCount();
		for (int num = 1; num < numCh; num++) {
			ASTNode child = (ASTNode) ast.getChild(num);
			if (storageFormat.fillStorageFormat(child)) {
				continue;
			}
			switch (child.getToken().getType()) {
				case HiveASTParser.TOK_IFNOTEXISTS:
					ifNotExists = true;
					break;
				case HiveASTParser.TOK_REWRITE_ENABLED:
					rewriteEnabled = true;
					break;
				case HiveASTParser.TOK_ORREPLACE:
					orReplace = true;
					break;
				case HiveASTParser.TOK_QUERY:
					selectStmt = child;
					break;
				case HiveASTParser.TOK_TABCOLNAME:
					cols = getColumns(child);
					break;
				case HiveASTParser.TOK_TABLECOMMENT:
					comment = unescapeSQLString(child.getChild(0).getText());
					break;
				case HiveASTParser.TOK_TABLEPROPERTIES:
					tblProps = getProps((ASTNode) child.getChild(0));
					break;
				case HiveASTParser.TOK_TABLEROWFORMAT:
					rowFormatParams.analyzeRowFormat(child);
					break;
				case HiveASTParser.TOK_TABLESERIALIZER:
					child = (ASTNode) child.getChild(0);
					storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
					if (child.getChildCount() == 2) {
						readProps((ASTNode) (child.getChild(1).getChild(0)),
								storageFormat.getSerdeProps());
					}
					break;
				case HiveASTParser.TOK_TABLELOCATION:
				case HiveASTParser.TOK_VIEWPARTCOLS:
				default:
					assert false;
			}
		}

		storageFormat.fillDefaultStorageFormat(false, isMaterialized);

		if (ifNotExists && orReplace) {
			throw new SemanticException("Can't combine IF NOT EXISTS and OR REPLACE.");
		}

		if (ast.getToken().getType() == HiveASTParser.TOK_ALTERVIEW &&
				ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
			isAlterViewAs = true;
			orReplace = true;
		}

		queryState.setCommandType(HiveOperation.CREATEVIEW);
		return new HiveParserCreateViewDesc(dbDotTable, cols, comment, tblProps, ifNotExists, isAlterViewAs, selectStmt);
	}

	private Serializable analyzeCreateTable(ASTNode ast) throws SemanticException {
		String[] qualifiedTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
		String dbDotTab = getDotName(qualifiedTabName);

		String likeTableName = null;
		List<FieldSchema> cols = new ArrayList<>();
		List<FieldSchema> partCols = new ArrayList<>();
		List<String> bucketCols = new ArrayList<>();
		List<PrimaryKey> primaryKeys = new ArrayList<>();
//		List<SQLForeignKey> foreignKeys = new ArrayList<>();
		List<Order> sortCols = new ArrayList<>();
		int numBuckets = -1;
		String comment = null;
		String location = null;
		Map<String, String> tblProps = null;
		boolean ifNotExists = false;
		boolean isExt = false;
		boolean isTemporary = false;
		boolean isMaterialization = false;
		ASTNode selectStmt = null;
		final int createTable = 0; // regular CREATE TABLE
		final int ctlt = 1; // CREATE TABLE LIKE ... (CTLT)
		final int ctas = 2; // CREATE TABLE AS SELECT ... (CTAS)
		int commandType = createTable;
		List<String> skewedColNames = new ArrayList<>();
		List<List<String>> skewedValues = new ArrayList<>();
		boolean storedAsDirs = false;
		boolean isUserStorageFormat = false;

		HiveParserRowFormatParams rowFormatParams = new HiveParserRowFormatParams();
		HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

		LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
		int numCh = ast.getChildCount();

		// Check the 1st-level children and do simple semantic checks: 1) CTLT and CTAS should not coexists.
		// 2) CTLT or CTAS should not coexists with column list (target table schema).
		// 3) CTAS does not support partitioning (for now).
		for (int num = 1; num < numCh; num++) {
			ASTNode child = (ASTNode) ast.getChild(num);
			if (storageFormat.fillStorageFormat(child)) {
				isUserStorageFormat = true;
				continue;
			}
			switch (child.getToken().getType()) {
				case HiveASTParser.TOK_IFNOTEXISTS:
					ifNotExists = true;
					break;
				case HiveASTParser.KW_EXTERNAL:
					isExt = true;
					break;
				case HiveASTParser.KW_TEMPORARY:
					isTemporary = true;
					isMaterialization = MATERIALIZATION_MARKER.equals(child.getText());
					break;
				case HiveASTParser.TOK_LIKETABLE:
					if (child.getChildCount() > 0) {
						likeTableName = getUnescapedName((ASTNode) child.getChild(0));
						if (likeTableName != null) {
							if (commandType == ctas) {
								throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
							}
							if (cols.size() != 0) {
								throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
							}
						}
						commandType = ctlt;
					}
					break;

				case HiveASTParser.TOK_QUERY: // CTAS
					if (commandType == ctlt) {
						throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
					}
					if (cols.size() != 0) {
						throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
					}
					if (partCols.size() != 0 || bucketCols.size() != 0) {
						throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
					}
					if (isExt) {
						throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
					}
					commandType = ctas;
					selectStmt = child;
					break;
				case HiveASTParser.TOK_TABCOLLIST:
					cols = getColumns(child, true, primaryKeys);
					break;
				case HiveASTParser.TOK_TABLECOMMENT:
					comment = unescapeSQLString(child.getChild(0).getText());
					break;
				case HiveASTParser.TOK_TABLEPARTCOLS:
					partCols = getColumns((ASTNode) child.getChild(0), false);
					break;
				case HiveASTParser.TOK_TABLEROWFORMAT:
					rowFormatParams.analyzeRowFormat(child);
					break;
				case HiveASTParser.TOK_TABLELOCATION:
					location = unescapeSQLString(child.getChild(0).getText());
					location = EximUtil.relativeToAbsolutePath(conf, location);
					break;
				case HiveASTParser.TOK_TABLEPROPERTIES:
					tblProps = getProps((ASTNode) child.getChild(0));
					break;
				case HiveASTParser.TOK_TABLESERIALIZER:
					child = (ASTNode) child.getChild(0);
					storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
					if (child.getChildCount() == 2) {
						readProps((ASTNode) (child.getChild(1).getChild(0)),
								storageFormat.getSerdeProps());
					}
					break;
				case HiveASTParser.TOK_ALTERTABLE_BUCKETS:
				case HiveASTParser.TOK_TABLESKEWED:
				default:
					throw new AssertionError("Unknown token: " + child.getToken());
			}
		}

		if (commandType == createTable || commandType == ctlt) {
			queryState.setCommandType(HiveOperation.CREATETABLE);
		} else if (commandType == ctas) {
			queryState.setCommandType(HiveOperation.CREATETABLE_AS_SELECT);
		} else {
			throw new SemanticException("Unrecognized command.");
		}

		storageFormat.fillDefaultStorageFormat(isExt, false);

		if (isTemporary) {
			if (partCols.size() > 0) {
				throw new SemanticException("Partition columns are not supported on temporary tables");
			}
		}

		// Handle different types of CREATE TABLE command
		switch (commandType) {

			case createTable: // REGULAR CREATE TABLE DDL
				tblProps = addDefaultProperties(tblProps);

//				CreateTableDesc crtTblDesc = new CreateTableDesc(dbDotTab, isExt, isTemporary, cols, partCols,
//						bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
//						rowFormatParams.fieldEscape,
//						rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim, rowFormatParams.lineDelim, comment,
//						storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
//						storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames,
//						skewedValues, primaryKeys, foreignKeys);
//				crtTblDesc.setStoredAsSubDirectories(storedAsDirs);
//				crtTblDesc.setNullFormat(rowFormatParams.nullFormat);

//				crtTblDesc.validate(conf);
				return new HiveParserCreateTableDesc(dbDotTab, isExt, ifNotExists, isTemporary, cols, partCols,
						comment, location, tblProps, rowFormatParams, storageFormat, primaryKeys);

			case ctlt: // create table like <tbl_name>
				tblProps = addDefaultProperties(tblProps);

				// TODO: check this somewhere else
//				if (isTemporary) {
//					Table likeTable = getTable(likeTableName, false);
//					if (likeTable != null && likeTable.getPartCols().size() > 0) {
//						throw new SemanticException("Partition columns are not supported on temporary tables "
//								+ "and source table in CREATE TABLE LIKE is partitioned.");
//					}
//				}
				CreateTableLikeDesc crtTblLikeDesc = new CreateTableLikeDesc(dbDotTab, isExt, isTemporary,
						storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location,
						storageFormat.getSerde(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
						likeTableName, isUserStorageFormat);
				return new DDLWork(getInputs(), getOutputs(), crtTblLikeDesc);

			case ctas: // create table as select
				// TODO: check this somewhere else
//				if(location != null && location.length() != 0) {
//					Path locPath = new Path(location);
//					FileSystem curFs = null;
//					FileStatus locStats = null;
//					try {
//						curFs = locPath.getFileSystem(conf);
//						if(curFs != null) {
//							locStats = curFs.getFileStatus(locPath);
//						}
//						if(locStats != null && locStats.isDir()) {
//							FileStatus[] lStats = curFs.listStatus(locPath);
//							if(lStats != null && lStats.length != 0) {
//								// Don't throw an exception if the target location only contains the staging-dirs
//								for (FileStatus lStat : lStats) {
//									if (!lStat.getPath().getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR))) {
//										throw new SemanticException(ErrorMsg.CTAS_LOCATION_NONEMPTY.getMsg(location));
//									}
//								}
//							}
//						}
//					} catch (FileNotFoundException nfe) {
//						//we will create the folder if it does not exist.
//					} catch (IOException ioE) {
//						if (LOG.isDebugEnabled()) {
//							LOG.debug("Exception when validate folder ",ioE);
//						}
//
//					}
//				}

				tblProps = addDefaultProperties(tblProps);

				HiveParserCreateTableDesc createTableDesc = new HiveParserCreateTableDesc(dbDotTab, isExt, ifNotExists, isTemporary,
						cols, partCols, comment, location, tblProps, rowFormatParams, storageFormat, primaryKeys);

//				CreateTableDesc tableDesc = new CreateTableDesc(qualifiedTabName[0], dbDotTab, isExt, isTemporary, cols,
//						partCols, bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
//						rowFormatParams.fieldEscape, rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim,
//						rowFormatParams.lineDelim, comment, storageFormat.getInputFormat(),
//						storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
//						storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
//						skewedColNames, skewedValues, true, primaryKeys, foreignKeys);
//				tableDesc.setMaterialization(isMaterialization);
//				tableDesc.setStoredAsSubDirectories(storedAsDirs);
//				tableDesc.setNullFormat(rowFormatParams.nullFormat);
				return new CTASDesc(createTableDesc, selectStmt);
			default:
				throw new SemanticException("Unrecognized command.");
		}
	}

	private Serializable analyzeAlterTableUpdateStats(ASTNode ast, String tblName, Map<String, String> partSpec)
			throws SemanticException {
		String colName = getUnescapedName((ASTNode) ast.getChild(0));
		Map<String, String> mapProp = getProps((ASTNode) (ast.getChild(1)).getChild(0));

		Table tbl = getTable(tblName);
		String partName = null;
		if (partSpec != null) {
			try {
				partName = Warehouse.makePartName(partSpec, false);
			} catch (MetaException e) {
				throw new SemanticException("partition " + partSpec.toString() + " not found");
			}
		}

		String colType = null;
		List<FieldSchema> cols = tbl.getCols();
		for (FieldSchema col : cols) {
			if (colName.equalsIgnoreCase(col.getName())) {
				colType = col.getType();
				break;
			}
		}

		if (colType == null) {
			throw new SemanticException("column type not found");
		}

		ColumnStatsDesc cStatsDesc = new ColumnStatsDesc(tbl.getDbName() + "." + tbl.getTableName(),
				Collections.singletonList(colName), Collections.singletonList(colType), partSpec == null);
		return new ColumnStatsUpdateWork(cStatsDesc, partName, mapProp);
	}

	private Serializable analyzeAlterDatabaseProperties(ASTNode ast) throws SemanticException {

		String dbName = unescapeIdentifier(ast.getChild(0).getText());
		Map<String, String> dbProps = null;

		for (int i = 1; i < ast.getChildCount(); i++) {
			ASTNode childNode = (ASTNode) ast.getChild(i);
			switch (childNode.getToken().getType()) {
				case HiveASTParser.TOK_DATABASEPROPERTIES:
					dbProps = getProps((ASTNode) childNode.getChild(0));
					break;
				default:
					throw new SemanticException("Unrecognized token in CREATE DATABASE statement");
			}
		}
		AlterDatabaseDesc alterDesc = new AlterDatabaseDesc(dbName, dbProps);
		return new DDLWork(new HashSet<>(), new HashSet<>(), alterDesc);
	}

	private Serializable analyzeAlterDatabaseOwner(ASTNode ast) throws SemanticException {
		String dbName = getUnescapedName((ASTNode) ast.getChild(0));
		PrincipalDesc principalDesc = HiveParserAuthorizationParseUtils.getPrincipalDesc((ASTNode) ast.getChild(1));

		// The syntax should not allow these fields to be null, but lets verify
		String nullCmdMsg = "can't be null in alter database set owner command";
		if (principalDesc.getName() == null) {
			throw new SemanticException("Owner name " + nullCmdMsg);
		}
		if (principalDesc.getType() == null) {
			throw new SemanticException("Owner type " + nullCmdMsg);
		}

		AlterDatabaseDesc alterDesc = new AlterDatabaseDesc(dbName, principalDesc);
		return new DDLWork(new HashSet<>(), new HashSet<>(), alterDesc);
	}

	private Serializable analyzeCreateDatabase(ASTNode ast) throws SemanticException {
		String dbName = unescapeIdentifier(ast.getChild(0).getText());
		boolean ifNotExists = false;
		String dbComment = null;
		String dbLocation = null;
		Map<String, String> dbProps = null;

		for (int i = 1; i < ast.getChildCount(); i++) {
			ASTNode childNode = (ASTNode) ast.getChild(i);
			switch (childNode.getToken().getType()) {
				case HiveASTParser.TOK_IFNOTEXISTS:
					ifNotExists = true;
					break;
				case HiveASTParser.TOK_DATABASECOMMENT:
					dbComment = unescapeSQLString(childNode.getChild(0).getText());
					break;
				case HiveASTParser.TOK_DATABASEPROPERTIES:
					dbProps = getProps((ASTNode) childNode.getChild(0));
					break;
				case HiveASTParser.TOK_DATABASELOCATION:
					dbLocation = unescapeSQLString(childNode.getChild(0).getText());
					break;
				default:
					throw new SemanticException("Unrecognized token in CREATE DATABASE statement");
			}
		}

		CreateDatabaseDesc createDatabaseDesc = new CreateDatabaseDesc(dbName, dbComment, dbLocation, ifNotExists);
		if (dbProps != null) {
			createDatabaseDesc.setDatabaseProperties(dbProps);
		}
		return new DDLWork(getInputs(), getOutputs(), createDatabaseDesc);
	}

	private Serializable analyzeDropDatabase(ASTNode ast) throws SemanticException {
		String dbName = unescapeIdentifier(ast.getChild(0).getText());
		boolean ifExists = false;
		boolean ifCascade = false;

		if (null != ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS)) {
			ifExists = true;
		}

		if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
			ifCascade = true;
		}

		DropDatabaseDesc dropDatabaseDesc = new DropDatabaseDesc(dbName, ifExists, ifCascade);
		return new DDLWork(new HashSet<>(), new HashSet<>(), dropDatabaseDesc);
	}

	private Serializable analyzeSwitchDatabase(ASTNode ast) throws SemanticException {
		String dbName = unescapeIdentifier(ast.getChild(0).getText());
		SwitchDatabaseDesc switchDatabaseDesc = new SwitchDatabaseDesc(dbName);
		return new DDLWork(new HashSet<>(), new HashSet<>(), switchDatabaseDesc);
	}

	private Serializable analyzeDropTable(ASTNode ast, TableType expectedType) throws SemanticException {
		String tableName = getUnescapedName((ASTNode) ast.getChild(0));
		boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);

		ReplicationSpec replicationSpec = new ReplicationSpec(ast);

		boolean ifPurge = (ast.getFirstChildWithType(HiveASTParser.KW_PURGE) != null);
		return new HiveParserDropTableDesc(tableName, expectedType == TableType.VIRTUAL_VIEW, ifExists, ifPurge);
	}

	private static boolean isFullSpec(Table table, Map<String, String> partSpec) {
		for (FieldSchema partCol : table.getPartCols()) {
			if (partSpec.get(partCol.getName()) == null) {
				return false;
			}
		}
		return true;
	}

	private void validateAlterTableType(Table tbl, AlterTableDesc.AlterTableTypes op) throws SemanticException {
		validateAlterTableType(tbl, op, false);
	}

	private void validateAlterTableType(Table tbl, AlterTableDesc.AlterTableTypes op, boolean expectView)
			throws SemanticException {
		if (tbl.isView()) {
			if (!expectView) {
				throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_VIEWS.getMsg());
			}

			switch (op) {
				case ADDPARTITION:
				case DROPPARTITION:
				case RENAMEPARTITION:
				case ADDPROPS:
				case DROPPROPS:
				case RENAME:
					// allow this form
					break;
				default:
					throw new SemanticException(ErrorMsg.ALTER_VIEW_DISALLOWED_OP.getMsg(op.toString()));
			}
		} else {
			if (expectView) {
				throw new SemanticException(ErrorMsg.ALTER_COMMAND_FOR_TABLES.getMsg());
			}
		}
		if (tbl.isNonNative()) {
			throw new SemanticException(ErrorMsg.ALTER_TABLE_NON_NATIVE.getMsg(tbl.getTableName()));
		}
	}

	private Serializable analyzeAlterTableProps(String[] qualified, HashMap<String, String> partSpec,
			ASTNode ast, boolean expectView, boolean isUnset) throws SemanticException {

		String tableName = getDotName(qualified);
		HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(0)).getChild(0));
		// we need to check if the properties are valid, especially for stats.
		// they might be changed via alter table .. update statistics or alter table .. set tblproperties.
		// If the property is not row_count or raw_data_size, it could not be changed through update statistics
		for (Map.Entry<String, String> entry : mapProp.entrySet()) {
			// we make sure that we do not change anything if there is anything wrong.
			if (entry.getKey().equals(StatsSetupConst.ROW_COUNT) || entry.getKey().equals(StatsSetupConst.RAW_DATA_SIZE)) {
				try {
					Long.parseLong(entry.getValue());
				} catch (Exception e) {
					throw new SemanticException("AlterTable " + entry.getKey() + " failed with value "
							+ entry.getValue());
				}
			} else {
				if (HiveOperation.ALTERTABLE_UPDATETABLESTATS.getOperationName().equals(queryState.getCommandType())
						|| HiveOperation.ALTERTABLE_UPDATEPARTSTATS.getOperationName().equals(queryState.getCommandType())) {
					throw new SemanticException("AlterTable UpdateStats " + entry.getKey()
							+ " failed because the only valid keys are " + StatsSetupConst.ROW_COUNT + " and "
							+ StatsSetupConst.RAW_DATA_SIZE);
				}
			}
		}
		HiveParserAlterTableDesc alterTblDesc;
		if (isUnset) {
			throw new SemanticException("Unset properties not supported");
//			alterTblDesc = new AlterTableDesc(AlterTableDesc.AlterTableTypes.DROPPROPS, partSpec, expectView);
//			if (ast.getChild(1) != null) {
//				alterTblDesc.setDropIfExists(true);
//			}
		} else {
			alterTblDesc = HiveParserAlterTableDesc.alterTableProps(tableName, partSpec, mapProp, expectView);
		}

		return alterTblDesc;
	}

	private Serializable analyzeAlterTableSerdeProps(ASTNode ast, String tableName,
			HashMap<String, String> partSpec) throws SemanticException {
		HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(0)).getChild(0));
		return HiveParserAlterTableDesc.alterSerDe(tableName, partSpec, null, mapProp);
	}

	private Serializable analyzeAlterTableSerde(ASTNode ast, String tableName, HashMap<String, String> partSpec) {
		String serdeName = unescapeSQLString(ast.getChild(0).getText());
		HashMap<String, String> mapProp = null;
		if (ast.getChildCount() > 1) {
			mapProp = getProps((ASTNode) (ast.getChild(1)).getChild(0));
		}
		return HiveParserAlterTableDesc.alterSerDe(tableName, partSpec, serdeName, mapProp);
	}

	private Serializable analyzeAlterTableFileFormat(ASTNode ast, String tableName, HashMap<String, String> partSpec)
			throws SemanticException {

		HiveParserStorageFormat format = new HiveParserStorageFormat(conf);
		ASTNode child = (ASTNode) ast.getChild(0);

		if (!format.fillStorageFormat(child)) {
			throw new AssertionError("Unknown token " + child.getText());
		}

		HiveParserAlterTableDesc alterTblDesc = HiveParserAlterTableDesc.alterFileFormat(tableName, partSpec);
		alterTblDesc.setGenericFileFormatName(format.getGenericName());

		return alterTblDesc;
	}

	private void addInputsOutputsAlterTable(String tableName, Map<String, String> partSpec,
			AlterTableDesc desc) throws SemanticException {
		addInputsOutputsAlterTable(tableName, partSpec, desc, desc.getOp());
	}

	private void addInputsOutputsAlterTable(String tableName, Map<String, String> partSpec,
			AlterTableDesc desc, AlterTableDesc.AlterTableTypes op) throws SemanticException {
		boolean isCascade = desc != null && desc.getIsCascade();
		boolean alterPartitions = partSpec != null && !partSpec.isEmpty();
		// cascade only occurs at table level then cascade to partition level
		if (isCascade && alterPartitions) {
			throw new SemanticException(ErrorMsg.ALTER_TABLE_PARTITION_CASCADE_NOT_SUPPORTED, op.getName());
		}

		Table tab = getTable(tableName);

		if (desc != null) {
			validateAlterTableType(tab, op, desc.getExpectView());

			// validate Unset Non Existed Table Properties
			if (op == AlterTableDesc.AlterTableTypes.DROPPROPS && !desc.getIsDropIfExists()) {
				Map<String, String> tableParams = tab.getTTable().getParameters();
				for (String currKey : desc.getProps().keySet()) {
					if (!tableParams.containsKey(currKey)) {
						String errorMsg = "The following property " + currKey + " does not exist in " + tab.getTableName();
						throw new SemanticException(ErrorMsg.ALTER_TBL_UNSET_NON_EXIST_PROPERTY.getMsg(errorMsg));
					}
				}
			}
		}
	}

	private Serializable analyzeAlterTableLocation(ASTNode ast, String tableName, HashMap<String, String> partSpec) throws SemanticException {
		String newLocation = unescapeSQLString(ast.getChild(0).getText());
		return HiveParserAlterTableDesc.alterLocation(tableName, partSpec, newLocation);
	}

	public static HashMap<String, String> getProps(ASTNode prop) {
		// Must be deterministic order map for consistent q-test output across Java versions
		HashMap<String, String> mapProp = new LinkedHashMap<>();
		readProps(prop, mapProp);
		return mapProp;
	}

	/**
	 * Utility class to resolve QualifiedName.
	 */
	private static class QualifiedNameUtil {

		// Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT^(DOT a b) c) will generate a name of the form a.b.c
		public static String getFullyQualifiedName(ASTNode ast) {
			if (ast.getChildCount() == 0) {
				return ast.getText();
			} else if (ast.getChildCount() == 2) {
				return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
						+ getFullyQualifiedName((ASTNode) ast.getChild(1));
			} else if (ast.getChildCount() == 3) {
				return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
						+ getFullyQualifiedName((ASTNode) ast.getChild(1)) + "."
						+ getFullyQualifiedName((ASTNode) ast.getChild(2));
			} else {
				return null;
			}
		}

		// get the column path
		// return column name if exists, column could be DOT separated.
		// example: lintString.$elem$.myint
		// return table name for column name if no column has been specified.
		public static String getColPath(
				ASTNode node,
				String dbName,
				String tableName,
				Map<String, String> partSpec) throws SemanticException {

			// if this ast has only one child, then no column name specified.
			if (node.getChildCount() == 1) {
				return tableName;
			}

			ASTNode columnNode = null;
			// Second child node could be partitionspec or column
			if (node.getChildCount() > 1) {
				if (partSpec == null) {
					columnNode = (ASTNode) node.getChild(1);
				} else {
					columnNode = (ASTNode) node.getChild(2);
				}
			}

			if (columnNode != null) {
				if (dbName == null) {
					return tableName + "." + QualifiedNameUtil.getFullyQualifiedName(columnNode);
				} else {
					return tableName.substring(dbName.length() + 1, tableName.length()) + "." +
							QualifiedNameUtil.getFullyQualifiedName(columnNode);
				}
			} else {
				return tableName;
			}
		}

		// get partition metadata
		public static Map<String, String> getPartitionSpec(HiveCatalog db, ASTNode ast, ObjectPath tablePath)
				throws SemanticException {
			ASTNode partNode = null;
			// if this ast has only one child, then no partition spec specified.
			if (ast.getChildCount() == 1) {
				return null;
			}

			// if ast has two children
			// the 2nd child could be partition spec or columnName
			// if the ast has 3 children, the second *has to* be partition spec
			if (ast.getChildCount() > 2 && (ast.getChild(1).getType() != HiveASTParser.TOK_PARTSPEC)) {
				throw new SemanticException(ast.getChild(1).getType() + " is not a partition specification");
			}

			if (ast.getChild(1).getType() == HiveASTParser.TOK_PARTSPEC) {
				partNode = (ASTNode) ast.getChild(1);
			}

			if (partNode != null) {
				Table tab;
				try {
					tab = new Table(db.getHiveTable(tablePath));
				} catch (TableNotExistException e) {
					throw new SemanticException(e);
				}

				HashMap<String, String> partSpec;
				try {
					partSpec = getValidatedPartSpec(tab, partNode, db.getHiveConf(), false);
				} catch (SemanticException e) {
					// get exception in resolving partition
					// it could be DESCRIBE table key
					// return null
					// continue processing for DESCRIBE table key
					return null;
				}

				if (partSpec != null) {
					if (!db.partitionExists(tablePath, new CatalogPartitionSpec(partSpec))) {
						throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partSpec.toString()));
					}
					return partSpec;
				}
			}

			return null;
		}

	}

	private void validateDatabase(String databaseName) throws SemanticException {
		try {
			if (!hiveCatalog.databaseExists(databaseName)) {
				throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName));
			}
		} catch (CatalogException e) {
			throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName), e);
		}
	}

	private void validateTable(String tableName, Map<String, String> partSpec) throws SemanticException {
		Table tab = getTable(tableName);
		if (partSpec != null) {
			getPartition(tab, partSpec);
		}
	}

	private void getPartition(Table table, Map<String, String> partSpec) throws SemanticException {
		try {
			hiveCatalog.getPartition(new ObjectPath(table.getDbName(), table.getTableName()), new CatalogPartitionSpec(partSpec));
		} catch (PartitionNotExistException e) {
			throw new SemanticException(e);
		}
	}

	/**
	 * A query like this will generate a tree as follows
	 * "describe formatted default.maptable partition (b=100) id;"
	 * TOK_TABTYPE
	 * TOK_TABNAME --> root for tablename, 2 child nodes mean DB specified
	 * default
	 * maptable
	 * TOK_PARTSPEC  --> root node for partition spec. else columnName
	 * TOK_PARTVAL
	 * b
	 * 100
	 * id           --> root node for columnName
	 * formatted
	 */
	private Serializable analyzeDescribeTable(ASTNode ast) throws SemanticException {
		ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);

		String dbName = null;
		String tableName;
		String colPath;
		Map<String, String> partSpec;

		ASTNode tableNode;

		// process the first node to extract tablename
		// tablename is either TABLENAME or DBNAME.TABLENAME if db is given
		if (tableTypeExpr.getChild(0).getType() == HiveASTParser.TOK_TABNAME) {
			tableNode = (ASTNode) tableTypeExpr.getChild(0);
			if (tableNode.getChildCount() == 1) {
				tableName = tableNode.getChild(0).getText();
			} else {
				dbName = tableNode.getChild(0).getText();
				tableName = dbName + "." + tableNode.getChild(1).getText();
			}
		} else {
			throw new SemanticException(tableTypeExpr.getChild(0).getText() + " is not an expected token type");
		}

		// process the second child,if exists, node to get partition spec(s)
		partSpec = QualifiedNameUtil.getPartitionSpec(hiveCatalog, tableTypeExpr, toObjectPath(tableName));

		// process the third child node,if exists, to get partition spec(s)
		colPath = QualifiedNameUtil.getColPath(tableTypeExpr, dbName, tableName, partSpec);

		// if database is not the one currently using
		// validate database
		if (dbName != null) {
			validateDatabase(dbName);
		}
		if (partSpec != null) {
			validateTable(tableName, partSpec);
		}

		DescTableDesc descTblDesc = new DescTableDesc(ctx.getResFile(), tableName, partSpec, colPath);

		boolean showColStats = false;
		if (ast.getChildCount() == 2) {
			int descOptions = ast.getChild(1).getType();
			descTblDesc.setFormatted(descOptions == HiveASTParser.KW_FORMATTED);
			descTblDesc.setExt(descOptions == HiveASTParser.KW_EXTENDED);
			descTblDesc.setPretty(descOptions == HiveASTParser.KW_PRETTY);
			// in case of "DESCRIBE FORMATTED tablename column_name" statement, colPath
			// will contain tablename.column_name. If column_name is not specified
			// colPath will be equal to tableName. This is how we can differentiate
			// if we are describing a table or column
			if (!colPath.equalsIgnoreCase(tableName) && descTblDesc.isFormatted()) {
				showColStats = true;
			}
		}

		return new DDLWork(getInputs(), getOutputs(), descTblDesc);
	}

	private Serializable analyzeDescDatabase(ASTNode ast) throws SemanticException {

		boolean isExtended;
		String dbName;

		if (ast.getChildCount() == 1) {
			dbName = stripQuotes(ast.getChild(0).getText());
			isExtended = false;
		} else if (ast.getChildCount() == 2) {
			dbName = stripQuotes(ast.getChild(0).getText());
			isExtended = true;
		} else {
			throw new SemanticException("Unexpected Tokens at DESCRIBE DATABASE");
		}

		DescDatabaseDesc descDbDesc = new DescDatabaseDesc(ctx.getResFile(), dbName, isExtended);
		return new DDLWork(getInputs(), getOutputs(), descDbDesc);
	}

	public static HashMap<String, String> getPartSpec(ASTNode partspec) {
		if (partspec == null) {
			return null;
		}
		HashMap<String, String> partSpec = new LinkedHashMap<>();
		for (int i = 0; i < partspec.getChildCount(); ++i) {
			ASTNode partVal = (ASTNode) partspec.getChild(i);
			String key = partVal.getChild(0).getText();
			String val = null;
			if (partVal.getChildCount() == 3) {
				val = stripQuotes(partVal.getChild(2).getText());
			} else if (partVal.getChildCount() == 2) {
				val = stripQuotes(partVal.getChild(1).getText());
			}
			partSpec.put(key.toLowerCase(), val);
		}
		return partSpec;
	}

	public static HashMap<String, String> getValidatedPartSpec(Table table, ASTNode astNode,
			HiveConf conf, boolean shouldBeFull) throws SemanticException {
		HashMap<String, String> partSpec = getPartSpec(astNode);
		// hive catalog will validate the part spec later
//		if (partSpec != null && !partSpec.isEmpty()) {
//			validatePartSpec(table, partSpec, astNode, conf, shouldBeFull);
//		}
		return partSpec;
	}

	private Serializable analyzeShowPartitions(ASTNode ast) throws SemanticException {
		ShowPartitionsDesc showPartsDesc;
		String tableName = getUnescapedName((ASTNode) ast.getChild(0));
		List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);
		// We only can have a single partition spec
		assert (partSpecs.size() <= 1);
		Map<String, String> partSpec = null;
		if (partSpecs.size() > 0) {
			partSpec = partSpecs.get(0);
		}

		validateTable(tableName, null);

		showPartsDesc = new ShowPartitionsDesc(tableName, ctx.getResFile(), partSpec);
		return new DDLWork(getInputs(), getOutputs(), showPartsDesc);
	}

//	private Serializable analyzeShowCreateDatabase(ASTNode ast) throws SemanticException {
//		String dbName = getUnescapedName((ASTNode) ast.getChild(0));
//		ShowCreateDatabaseDesc showCreateDbDesc = new ShowCreateDatabaseDesc(dbName, ctx.getResFile().toString());
//
//		return new DDLWork(getInputs(), getOutputs(), showCreateDbDesc);
//	}

	private Serializable analyzeShowCreateTable(ASTNode ast) throws SemanticException {
		ShowCreateTableDesc showCreateTblDesc;
		String tableName = getUnescapedName((ASTNode) ast.getChild(0));
		showCreateTblDesc = new ShowCreateTableDesc(tableName, ctx.getResFile().toString());

		Table tab = getTable(tableName);
		if (tab.getTableType() == org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE) {
			throw new SemanticException(ErrorMsg.SHOW_CREATETABLE_INDEX.getMsg(tableName
					+ " has table type INDEX_TABLE"));
		}
		return new DDLWork(getInputs(), getOutputs(), showCreateTblDesc);
	}

	private Serializable analyzeShowDatabases(ASTNode ast) throws SemanticException {
		ShowDatabasesDesc showDatabasesDesc;
		if (ast.getChildCount() == 1) {
			String databasePattern = unescapeSQLString(ast.getChild(0).getText());
			showDatabasesDesc = new ShowDatabasesDesc(ctx.getResFile(), databasePattern);
		} else {
			showDatabasesDesc = new ShowDatabasesDesc(ctx.getResFile());
		}
		return new DDLWork(getInputs(), getOutputs(), showDatabasesDesc);
	}

	private Serializable analyzeShowTables(ASTNode ast) throws SemanticException {
		String dbName = currentDB;
		String pattern = null;

		if (ast.getChildCount() > 3) {
			throw new SemanticException("Internal error : Invalid AST " + ast.toStringTree());
		}

		switch (ast.getChildCount()) {
			case 1: // Uses a pattern
				pattern = unescapeSQLString(ast.getChild(0).getText());
				break;
			case 2: // Specifies a DB
				assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
				dbName = unescapeIdentifier(ast.getChild(1).getText());
				validateDatabase(dbName);
				break;
			case 3: // Uses a pattern and specifies a DB
				assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
				dbName = unescapeIdentifier(ast.getChild(1).getText());
				pattern = unescapeSQLString(ast.getChild(2).getText());
				validateDatabase(dbName);
				break;
			default: // No pattern or DB
				break;
		}
		return new HiveParserShowTablesDesc(pattern, dbName, false);
	}

	private Serializable analyzeShowColumns(ASTNode ast) throws SemanticException {
		String tableName = getUnescapedName((ASTNode) ast.getChild(0));
		if (ast.getChildCount() > 1) {
			if (tableName.contains(".")) {
				throw new SemanticException("Duplicates declaration for database name");
			}
			tableName = getUnescapedName((ASTNode) ast.getChild(1)) + "." + tableName;
		}

		ShowColumnsDesc showColumnsDesc = new ShowColumnsDesc(ctx.getResFile(), tableName);
		return new DDLWork(getInputs(), getOutputs(), showColumnsDesc);
	}

	private Serializable analyzeShowTableStatus(ASTNode ast) throws SemanticException {
		ShowTableStatusDesc showTblStatusDesc;
		String tableNames = getUnescapedName((ASTNode) ast.getChild(0));
		String dbName = currentDB;
		int children = ast.getChildCount();
		HashMap<String, String> partSpec = null;
		if (children >= 2) {
			if (children > 3) {
				throw new SemanticException("Internal error : Invalid AST");
			}
			for (int i = 1; i < children; i++) {
				ASTNode child = (ASTNode) ast.getChild(i);
				if (child.getToken().getType() == HiveASTParser.Identifier) {
					dbName = unescapeIdentifier(child.getText());
				} else if (child.getToken().getType() == HiveASTParser.TOK_PARTSPEC) {
					partSpec = getValidatedPartSpec(getTable(tableNames), child, conf, false);
				} else {
					throw new SemanticException("Internal error : Invalid AST " + child.toStringTree() +
							" , Invalid token " + child.getToken().getType());
				}
			}
		}

		if (partSpec != null) {
			validateTable(tableNames, partSpec);
		}

		showTblStatusDesc = new ShowTableStatusDesc(ctx.getResFile().toString(), dbName, tableNames, partSpec);
		return new DDLWork(getInputs(), getOutputs(), showTblStatusDesc);
	}

	private Serializable analyzeShowTableProperties(ASTNode ast) throws SemanticException {
		ShowTblPropertiesDesc showTblPropertiesDesc;
		String[] qualified = getQualifiedTableName((ASTNode) ast.getChild(0));
		String propertyName = null;
		if (ast.getChildCount() > 1) {
			propertyName = unescapeSQLString(ast.getChild(1).getText());
		}

		String tableNames = getDotName(qualified);
		validateTable(tableNames, null);

		showTblPropertiesDesc = new ShowTblPropertiesDesc(ctx.getResFile().toString(), tableNames, propertyName);
		return new DDLWork(getInputs(), getOutputs(), showTblPropertiesDesc);
	}

	/**
	 * Add the task according to the parsed command tree. This is used for the CLI
	 * command "SHOW FUNCTIONS;".
	 *
	 * @param ast The parsed command tree.
	 * @throws SemanticException Parsin failed
	 */
	private Serializable analyzeShowFunctions(ASTNode ast) throws SemanticException {
		ShowFunctionsDesc showFuncsDesc;
		if (ast.getChildCount() == 1) {
			String funcNames = stripQuotes(ast.getChild(0).getText());
			showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile(), funcNames);
		} else if (ast.getChildCount() == 2) {
			assert (ast.getChild(0).getType() == HiveASTParser.KW_LIKE);
			String funcNames = stripQuotes(ast.getChild(1).getText());
			showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile(), funcNames, true);
		} else {
			showFuncsDesc = new ShowFunctionsDesc(ctx.getResFile());
		}
		return new DDLWork(getInputs(), getOutputs(), showFuncsDesc);
	}

	private Serializable analyzeShowConf(ASTNode ast) throws SemanticException {
		String confName = stripQuotes(ast.getChild(0).getText());
		ShowConfDesc showConfDesc = new ShowConfDesc(ctx.getResFile(), confName);
		return new DDLWork(getInputs(), getOutputs(), showConfDesc);
	}

	private Serializable analyzeShowViews(ASTNode ast) throws SemanticException {
		String dbName = currentDB;
		String pattern = null;

		if (ast.getChildCount() > 3) {
			throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
		}

		switch (ast.getChildCount()) {
			case 1: // Uses a pattern
				pattern = unescapeSQLString(ast.getChild(0).getText());
				break;
			case 2: // Specifies a DB
				assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
				dbName = unescapeIdentifier(ast.getChild(1).getText());
				validateDatabase(dbName);
				break;
			case 3: // Uses a pattern and specifies a DB
				assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
				dbName = unescapeIdentifier(ast.getChild(1).getText());
				pattern = unescapeSQLString(ast.getChild(2).getText());
				validateDatabase(dbName);
				break;
			default: // No pattern or DB
				break;
		}

		return new HiveParserShowTablesDesc(pattern, dbName, true);
	}

	/**
	 * Add the task according to the parsed command tree. This is used for the CLI
	 * command "DESCRIBE FUNCTION;".
	 *
	 * @param ast The parsed command tree.
	 * @throws SemanticException Parsing failed
	 */
	private Serializable analyzeDescFunction(ASTNode ast) throws SemanticException {
		String funcName;
		boolean isExtended;

		if (ast.getChildCount() == 1) {
			funcName = stripQuotes(ast.getChild(0).getText());
			isExtended = false;
		} else if (ast.getChildCount() == 2) {
			funcName = stripQuotes(ast.getChild(0).getText());
			isExtended = true;
		} else {
			throw new SemanticException("Unexpected Tokens at DESCRIBE FUNCTION");
		}

		DescFunctionDesc descFuncDesc = new DescFunctionDesc(ctx.getResFile(), funcName, isExtended);
		return new DDLWork(getInputs(), getOutputs(), descFuncDesc);
	}

	private Serializable analyzeAlterTableRename(String[] source, ASTNode ast, boolean expectView)
			throws SemanticException {
		String[] target = getQualifiedTableName((ASTNode) ast.getChild(0));

		String sourceName = getDotName(source);
		String targetName = getDotName(target);

		return HiveParserAlterTableDesc.rename(sourceName, targetName, expectView);
	}

	private Serializable analyzeAlterTableRenameCol(String[] qualified, ASTNode ast,
			HashMap<String, String> partSpec) throws SemanticException {
		String newComment = null;
		boolean first = false;
		String flagCol = null;
		boolean isCascade = false;
		//col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT]
		String oldColName = ast.getChild(0).getText();
		String newColName = ast.getChild(1).getText();
		String newType = getTypeStringFromAST((ASTNode) ast.getChild(2));
		int childCount = ast.getChildCount();
		for (int i = 3; i < childCount; i++) {
			ASTNode child = (ASTNode) ast.getChild(i);
			switch (child.getToken().getType()) {
				case HiveASTParser.StringLiteral:
					newComment = unescapeSQLString(child.getText());
					break;
				case HiveASTParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
					flagCol = unescapeIdentifier(child.getChild(0).getText());
					break;
				case HiveASTParser.KW_FIRST:
					first = true;
					break;
				case HiveASTParser.TOK_CASCADE:
					isCascade = true;
					break;
				case HiveASTParser.TOK_RESTRICT:
					break;
				default:
					throw new SemanticException("Unsupported token: " + child.getToken()
							+ " for alter table");
			}
		}

		// Validate the operation of renaming a column name.
		Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));

		SkewedInfo skewInfo = tab.getTTable().getSd().getSkewedInfo();
		if ((null != skewInfo)
				&& (null != skewInfo.getSkewedColNames())
				&& skewInfo.getSkewedColNames().contains(oldColName)) {
			throw new SemanticException(oldColName
					+ ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
		}

		String tblName = getDotName(qualified);
		return HiveParserAlterTableDesc.changeColumn(tblName, unescapeIdentifier(oldColName), unescapeIdentifier(newColName),
				newType, newComment, first, flagCol, isCascade);
	}

	private Serializable analyzeAlterTableRenamePart(ASTNode ast, String tblName,
			HashMap<String, String> oldPartSpec) throws SemanticException {
		Table tab = getTable(tblName);
		validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.RENAMEPARTITION);
		Map<String, String> newPartSpec =
				getValidatedPartSpec(tab, (ASTNode) ast.getChild(0), conf, false);
		if (newPartSpec == null) {
			throw new SemanticException("RENAME PARTITION Missing Destination" + ast);
		}

		RenamePartitionDesc renamePartitionDesc = new RenamePartitionDesc(tblName, oldPartSpec, newPartSpec);
		return new DDLWork(getInputs(), getOutputs(), renamePartitionDesc);
	}

	private Serializable analyzeAlterTableModifyCols(String[] qualified, ASTNode ast,
			HashMap<String, String> partSpec, boolean replace) throws SemanticException {

		String tblName = getDotName(qualified);
		List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(0));
		boolean isCascade = false;
		if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
			isCascade = true;
		}

		return HiveParserAlterTableDesc.addReplaceColumns(tblName, newCols, replace, isCascade);
	}

	private Serializable analyzeAlterTableDropParts(String[] qualified, ASTNode ast, boolean expectView) throws SemanticException {

		boolean ifExists = ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null;
		// If the drop has to fail on non-existent partitions, we cannot batch expressions.
		// That is because we actually have to check each separate expression for existence.
		// We could do a small optimization for the case where expr has all columns and all
		// operators are equality, if we assume those would always match one partition (which
		// may not be true with legacy, non-normalized column values). This is probably a
		// popular case but that's kinda hacky. Let's not do it for now.

		boolean mustPurge = (ast.getFirstChildWithType(HiveASTParser.KW_PURGE) != null);
		ReplicationSpec replicationSpec = new ReplicationSpec(ast);

		Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
		// hive represents drop partition specs with generic func desc, but what we need is just spec maps
//		Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs = getFullPartitionSpecs(ast, tab, ifExists);
		List<Map<String, String>> partSpecs = new ArrayList<>();
		for (int i = 0; i < ast.getChildCount(); i++) {
			ASTNode child = (ASTNode) ast.getChild(i);
			if (child.getType() == HiveASTParser.TOK_PARTSPEC) {
				partSpecs.add(getPartSpec(child));
			}
		}

		validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.DROPPARTITION, expectView);

		return new DropPartitionDesc(qualified[0], qualified[1], partSpecs, ifExists);
	}

	private Serializable analyzeAlterTablePartColType(String[] qualified, ASTNode ast) throws SemanticException {

		// check if table exists.
		Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));

		// validate the DDL is a valid operation on the table.
		validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.ALTERPARTITION, false);

		// Alter table ... partition column ( column newtype) only takes one column at a time.
		// It must have a column name followed with type.
		ASTNode colAst = (ASTNode) ast.getChild(0);

		FieldSchema newCol = new FieldSchema();

		// get column name
		String name = colAst.getChild(0).getText().toLowerCase();
		newCol.setName(unescapeIdentifier(name));

		// get column type
		ASTNode typeChild = (ASTNode) (colAst.getChild(1));
		newCol.setType(getTypeStringFromAST(typeChild));

		if (colAst.getChildCount() == 3) {
			newCol.setComment(unescapeSQLString(colAst.getChild(2).getText()));
		}

		// check if column is defined or not
		boolean fFoundColumn = false;
		for (FieldSchema col : tab.getTTable().getPartitionKeys()) {
			if (col.getName().compareTo(newCol.getName()) == 0) {
				fFoundColumn = true;
			}
		}

		// raise error if we could not find the column
		if (!fFoundColumn) {
			throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(newCol.getName()));
		}

		AlterTableAlterPartDesc alterTblAlterPartDesc = new AlterTableAlterPartDesc(getDotName(qualified), newCol);
		return new DDLWork(getInputs(), getOutputs(), alterTblAlterPartDesc);
	}

	/**
	 * Add one or more partitions to a table. Useful when the data has been copied
	 * to the right location by some other process.
	 *
	 * @param ast        The parsed command tree.
	 * @param expectView True for ALTER VIEW, false for ALTER TABLE.
	 * @throws SemanticException Parsing failed
	 */
	private Serializable analyzeAlterTableAddParts(String[] qualified, CommonTree ast, boolean expectView)
			throws SemanticException {

		// ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists? alterStatementSuffixAddPartitionsElement+)
		boolean ifNotExists = ast.getChild(0).getType() == HiveASTParser.TOK_IFNOTEXISTS;

		Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
		boolean isView = tab.isView();
		validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.ADDPARTITION, expectView);

		int numCh = ast.getChildCount();
		int start = ifNotExists ? 1 : 0;

		String currentLocation = null;
		Map<String, String> currentPart = null;
		// Parser has done some verification, so the order of tokens doesn't need to be verified here.
		AddPartitionDesc addPartitionDesc = new AddPartitionDesc(tab.getDbName(), tab.getTableName(), ifNotExists);
		for (int num = start; num < numCh; num++) {
			ASTNode child = (ASTNode) ast.getChild(num);
			switch (child.getToken().getType()) {
				case HiveASTParser.TOK_PARTSPEC:
					if (currentPart != null) {
						addPartitionDesc.addPartition(currentPart, currentLocation);
						currentLocation = null;
					}
					currentPart = getValidatedPartSpec(tab, child, conf, true);
					validatePartitionValues(currentPart); // validate reserved values
					break;
				case HiveASTParser.TOK_PARTITIONLOCATION:
					// if location specified, set in partition
					if (isView) {
						throw new SemanticException("LOCATION clause illegal for view partition");
					}
					currentLocation = unescapeSQLString(child.getChild(0).getText());
					break;
				default:
					throw new SemanticException("Unknown child: " + child);
			}
		}

		// add the last one
		if (currentPart != null) {
			addPartitionDesc.addPartition(currentPart, currentLocation);
		}

//		if (this.conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
//			for (int index = 0; index < addPartitionDesc.getPartitionCount(); index++) {
//				AddPartitionDesc.OnePartitionDesc desc = addPartitionDesc.getPartition(index);
//				if (desc.getLocation() == null) {
//					if (desc.getPartParams() == null) {
//						desc.setPartParams(new HashMap<>());
//					}
//					StatsSetupConst.setBasicStatsStateForCreateTable(desc.getPartParams(),
//							StatsSetupConst.TRUE);
//				}
//			}
//		}

		return new DDLWork(getInputs(), getOutputs(), addPartitionDesc);
	}

	// Get the partition specs from the tree
	private List<Map<String, String>> getPartitionSpecs(Table tbl, CommonTree ast) throws SemanticException {
		List<Map<String, String>> partSpecs = new ArrayList<>();
		// get partition metadata if partition specified
		for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
			ASTNode partSpecNode = (ASTNode) ast.getChild(childIndex);
			// sanity check
			if (partSpecNode.getType() == HiveASTParser.TOK_PARTSPEC) {
				Map<String, String> partSpec = getValidatedPartSpec(tbl, partSpecNode, conf, false);
				partSpecs.add(partSpec);
			}
		}
		return partSpecs;
	}

	/**
	 * Get the partition specs from the tree. This stores the full specification
	 * with the comparator operator into the output list.
	 *
	 * @param ast Tree to extract partitions from.
	 * @param tab Table.
	 * @return Map of partitions by prefix length. Most of the time prefix length will
	 * be the same for all partition specs, so we can just OR the expressions.
	 */
//	private Map<Integer, List<ExprNodeGenericFuncDesc>> getFullPartitionSpecs(
//			CommonTree ast, Table tab, boolean canGroupExprs) throws SemanticException {
//		String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
//		Map<String, String> colTypes = new HashMap<>();
//		for (FieldSchema fs : tab.getPartitionKeys()) {
//			colTypes.put(fs.getName().toLowerCase(), fs.getType());
//		}
//
//		Map<Integer, List<ExprNodeGenericFuncDesc>> result = new HashMap<>();
//		for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
//			Tree partSpecTree = ast.getChild(childIndex);
//			if (partSpecTree.getType() != HiveASTParser.TOK_PARTSPEC) {
//				continue;
//			}
//			ExprNodeGenericFuncDesc expr = null;
//			HashSet<String> names = new HashSet<>(partSpecTree.getChildCount());
//			for (int i = 0; i < partSpecTree.getChildCount(); ++i) {
//				CommonTree partSpecSingleKey = (CommonTree) partSpecTree.getChild(i);
//				assert (partSpecSingleKey.getType() == HiveASTParser.TOK_PARTVAL);
//				String key = stripIdentifierQuotes(partSpecSingleKey.getChild(0).getText()).toLowerCase();
//				String operator = partSpecSingleKey.getChild(1).getText();
//				ASTNode partValNode = (ASTNode) partSpecSingleKey.getChild(2);
//				HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(null);
//				ExprNodeConstantDesc valExpr = (ExprNodeConstantDesc) HiveParserTypeCheckProcFactory
//						.genExprNode(partValNode, typeCheckCtx).get(partValNode);
//				Object val = valExpr.getValue();
//
//				boolean isDefaultPartitionName = val.equals(defaultPartitionName);
//
//				String type = colTypes.get(key);
//				PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
//				if (type == null) {
//					throw new SemanticException("Column " + key + " not found");
//				}
//				// Create the corresponding hive expression to filter on partition columns.
//				if (!isDefaultPartitionName) {
//					if (!valExpr.getTypeString().equals(type)) {
//						ObjectInspectorConverters.Converter converter = ObjectInspectorConverters.getConverter(
//								TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valExpr.getTypeInfo()),
//								TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti));
//						val = converter.convert(valExpr.getValue());
//					}
//				}
//
//				ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
//				ExprNodeGenericFuncDesc op;
//				if (!isDefaultPartitionName) {
//					op = makeBinaryPredicate(operator, column, new ExprNodeConstantDesc(pti, val));
//				} else {
//					GenericUDF originalOp = HiveParserUtils.getFunctionInfo(operator).getGenericUDF();
//					String fnName;
//					if (originalOp instanceof GenericUDFOPEqual) {
//						fnName = "isnull";
//					} else if (originalOp instanceof GenericUDFOPNotEqual) {
//						fnName = "isnotnull";
//					} else {
//						throw new SemanticException("Cannot use " + operator
//								+ " in a default partition spec; only '=' and '!=' are allowed.");
//					}
//					op = makeUnaryPredicate(fnName, column);
//				}
//				// If it's multi-expr filter (e.g. a='5', b='2012-01-02'), AND with previous exprs.
//				expr = (expr == null) ? op : makeBinaryPredicate("and", expr, op);
//				names.add(key);
//			}
//			if (expr == null) {
//				continue;
//			}
//			// We got the expr for one full partition spec. Determine the prefix length.
//			int prefixLength = calculatePartPrefix(tab, names);
//			List<ExprNodeGenericFuncDesc> orExpr = result.get(prefixLength);
//			// We have to tell apart partitions resulting from spec with different prefix lengths.
//			// So, if we already have smth for the same prefix length, we can OR the two.
//			// If we don't, create a new separate filter. In most cases there will only be one.
//			if (orExpr == null) {
//				List<ExprNodeGenericFuncDesc> spec = new ArrayList<>();
//				spec.add(expr);
//				result.put(prefixLength, spec);
//			} else if (canGroupExprs) {
//				orExpr.set(0, makeBinaryPredicate("or", expr, orExpr.get(0)));
//			} else {
//				orExpr.add(expr);
//			}
//		}
//		return result;
//	}

//	public static ExprNodeGenericFuncDesc makeBinaryPredicate(
//			String fn, ExprNodeDesc left, ExprNodeDesc right) throws SemanticException {
//		return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
//				HiveParserUtils.getFunctionInfo(fn).getGenericUDF(), Arrays.asList(left, right));
//	}
//
//	public static ExprNodeGenericFuncDesc makeUnaryPredicate(
//			String fn, ExprNodeDesc arg) throws SemanticException {
//		return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
//				FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Collections.singletonList(arg));
//	}

	/**
	 * Calculates the partition prefix length based on the drop spec.
	 * This is used to avoid deleting archived partitions with lower level.
	 * For example, if, for A and B key cols, drop spec is A=5, B=6, we shouldn't drop
	 * archived A=5/, because it can contain B-s other than 6.
	 *
	 * @param tbl          Table
	 * @param partSpecKeys Keys present in drop partition spec.
	 */
//	private int calculatePartPrefix(Table tbl, HashSet<String> partSpecKeys) {
//		int partPrefixToDrop = 0;
//		for (FieldSchema fs : tbl.getPartCols()) {
//			if (!partSpecKeys.contains(fs.getName())) {
//				break;
//			}
//			++partPrefixToDrop;
//		}
//		return partPrefixToDrop;
//	}

	/**
	 * Certain partition values are are used by hive. e.g. the default partition
	 * in dynamic partitioning and the intermediate partition values used in the
	 * archiving process. Naturally, prohibit the user from creating partitions
	 * with these reserved values. The check that this function is more
	 * restrictive than the actual limitation, but it's simpler. Should be okay
	 * since the reserved names are fairly long and uncommon.
	 */
	private void validatePartitionValues(Map<String, String> partSpec) throws SemanticException {
		for (Map.Entry<String, String> e : partSpec.entrySet()) {
			for (String s : reservedPartitionValues) {
				String value = e.getValue();
				if (value != null && value.contains(s)) {
					throw new SemanticException(ErrorMsg.RESERVED_PART_VAL.getMsg(
							"(User value: " + e.getValue() + " Reserved substring: " + s + ")"));
				}
			}
		}
	}

	private Map<String, String> addDefaultProperties(Map<String, String> tblProp) {
		Map<String, String> retValue;
		if (tblProp == null) {
			retValue = new HashMap<>();
		} else {
			retValue = tblProp;
		}
		String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA);
		if (paraString != null && !paraString.isEmpty()) {
			for (String keyValuePair : paraString.split(",")) {
				String[] keyValue = keyValuePair.split("=", 2);
				if (keyValue.length != 2) {
					continue;
				}
				if (!retValue.containsKey(keyValue[0])) {
					retValue.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return retValue;
	}
}
