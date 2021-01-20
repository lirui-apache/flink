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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.table.planner.plan.nodes.hive.HiveDistribution;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.HiveParserContext;
import org.apache.hadoop.hive.ql.HiveParserQueryState;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserSqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserTypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveASTParseUtils;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;
import org.apache.hadoop.hive.ql.parse.HiveParserCalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParserQB;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A Parser that uses Hive's planner to parse a statement.
 */
public class HiveParser extends ParserImpl {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);

	// need to maintain the ASTNode types for DDLs
	private static final Set<Integer> DDL_NODES;

	static {
		DDL_NODES = new HashSet<>(Arrays.asList(HiveASTParser.TOK_ALTERTABLE, HiveASTParser.TOK_ALTERVIEW,
				HiveASTParser.TOK_CREATEDATABASE, HiveASTParser.TOK_DROPDATABASE, HiveASTParser.TOK_SWITCHDATABASE,
				HiveASTParser.TOK_DROPTABLE, HiveASTParser.TOK_DROPVIEW, HiveASTParser.TOK_DROP_MATERIALIZED_VIEW,
				HiveASTParser.TOK_DESCDATABASE, HiveASTParser.TOK_DESCTABLE, HiveASTParser.TOK_DESCFUNCTION,
				HiveASTParser.TOK_MSCK, HiveASTParser.TOK_ALTERINDEX_REBUILD, HiveASTParser.TOK_ALTERINDEX_PROPERTIES,
				HiveASTParser.TOK_SHOWDATABASES, HiveASTParser.TOK_SHOWTABLES, HiveASTParser.TOK_SHOWCOLUMNS,
				HiveASTParser.TOK_SHOW_TABLESTATUS, HiveASTParser.TOK_SHOW_TBLPROPERTIES, HiveASTParser.TOK_SHOW_CREATEDATABASE,
				HiveASTParser.TOK_SHOW_CREATETABLE, HiveASTParser.TOK_SHOWFUNCTIONS, HiveASTParser.TOK_SHOWPARTITIONS,
				HiveASTParser.TOK_SHOWINDEXES, HiveASTParser.TOK_SHOWLOCKS, HiveASTParser.TOK_SHOWDBLOCKS,
				HiveASTParser.TOK_SHOW_COMPACTIONS, HiveASTParser.TOK_SHOW_TRANSACTIONS, HiveASTParser.TOK_ABORT_TRANSACTIONS,
				HiveASTParser.TOK_SHOWCONF, HiveASTParser.TOK_SHOWVIEWS, HiveASTParser.TOK_CREATEINDEX, HiveASTParser.TOK_DROPINDEX,
				HiveASTParser.TOK_ALTERTABLE_CLUSTER_SORT, HiveASTParser.TOK_LOCKTABLE, HiveASTParser.TOK_UNLOCKTABLE,
				HiveASTParser.TOK_LOCKDB, HiveASTParser.TOK_UNLOCKDB, HiveASTParser.TOK_CREATEROLE, HiveASTParser.TOK_DROPROLE,
				HiveASTParser.TOK_GRANT, HiveASTParser.TOK_REVOKE, HiveASTParser.TOK_SHOW_GRANT, HiveASTParser.TOK_GRANT_ROLE,
				HiveASTParser.TOK_REVOKE_ROLE, HiveASTParser.TOK_SHOW_ROLE_GRANT, HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS,
				HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS, HiveASTParser.TOK_ALTERDATABASE_PROPERTIES, HiveASTParser.TOK_ALTERDATABASE_OWNER,
				HiveASTParser.TOK_TRUNCATETABLE, HiveASTParser.TOK_SHOW_SET_ROLE, HiveASTParser.TOK_CACHE_METADATA,
				HiveASTParser.TOK_CREATEMACRO, HiveASTParser.TOK_DROPMACRO, HiveASTParser.TOK_CREATETABLE,
				HiveASTParser.TOK_CREATEFUNCTION, HiveASTParser.TOK_DROPFUNCTION, HiveASTParser.TOK_RELOADFUNCTION,
				HiveASTParser.TOK_CREATEVIEW));
	}

	private final PlannerContext plannerContext;
	private final FlinkCalciteCatalogReader catalogReader;
	private final FrameworkConfig frameworkConfig;
	private final ConvertSqlFunctionCopier funcConverter;

	HiveParser(
			CatalogManager catalogManager,
			Supplier<FlinkPlannerImpl> validatorSupplier,
			Supplier<CalciteParser> calciteParserSupplier,
			Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator,
			PlannerContext plannerContext) {
		super(catalogManager, validatorSupplier, calciteParserSupplier, sqlExprToRexConverterCreator);
		this.plannerContext = plannerContext;
		this.catalogReader = plannerContext.createCatalogReader(
				false, catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
		this.frameworkConfig = plannerContext.createFrameworkConfig();
		this.funcConverter = new ConvertSqlFunctionCopier(
				plannerContext.getCluster().getRexBuilder(), frameworkConfig.getOperatorTable(), catalogReader.nameMatcher());
	}

	@Override
	public List<Operation> parse(String statement) {
		CatalogManager catalogManager = getCatalogManager();
		Catalog currentCatalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
		if (!(currentCatalog instanceof HiveCatalog)) {
			LOG.warn("Current catalog is not HiveCatalog. Falling back to Flink's planner.");
			return super.parse(statement);
		}
		HiveConf hiveConf = new HiveConf(((HiveCatalog) currentCatalog).getHiveConf());
		hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
		hiveConf.set("hive.allow.udf.load.on.demand", "false");
		HiveShim hiveShim = HiveShimLoader.loadHiveShim(((HiveCatalog) currentCatalog).getHiveVersion());
		try {
			// creates SessionState
			startSessionState(hiveConf, catalogManager);
			// We override Hive's grouping function. Refer to the implementation for more details.
			FunctionRegistry.registerTemporaryUDF("grouping", HiveGenericUDFGrouping.class);
			return processCmd(statement, hiveConf, hiveShim, (HiveCatalog) currentCatalog);
		} finally {
			clearSessionState(hiveConf);
		}
	}

	private List<Operation> processCmd(String cmd, HiveConf hiveConf, HiveShim hiveShim, HiveCatalog hiveCatalog) {
		try {
			final HiveParserContext context = new HiveParserContext(hiveConf);
			// parse statement to get AST
			final ASTNode node = HiveASTParseUtils.parse(cmd, context);
			// generate Calcite plan
			Operation operation;
			if (DDL_NODES.contains(node.getType())) {
				HiveParserQueryState queryState = new HiveParserQueryState(hiveConf);
				HiveParserDDLSemanticAnalyzer ddlAnalyzer = new HiveParserDDLSemanticAnalyzer(
						queryState, context, hiveCatalog, getCatalogManager().getCurrentDatabase());
				Serializable work = ddlAnalyzer.analyzeInternal(node);
				if (work == null) {
					return super.parse(cmd);
				} else {
					if (work instanceof HiveParserCreateViewDesc) {
						// analyze and expand the view query
						analyzeCreateView((HiveParserCreateViewDesc) work, context, queryState, hiveShim);
					} else if (work instanceof CTASDesc) {
						// analyze the query
						CTASDesc ctasDesc = (CTASDesc) work;
						HiveParserCalcitePlanner calcitePlanner = createCalcitePlanner(context, queryState, hiveShim);
						calcitePlanner.setCtasDesc(ctasDesc);
						RelNode queryRelNode = calcitePlanner.genLogicalPlan(ctasDesc.getQuery());
						// create a table to represent the dest table
						HiveParserCreateTableDesc createTableDesc = ctasDesc.getCreateTableDesc();
						String[] dbTblName = createTableDesc.getCompoundName().split("\\.");
						Table destTable = new Table(Table.getEmptyTable(dbTblName[0], dbTblName[1]));
						destTable.getSd().setCols(createTableDesc.getCols());
						// create the insert operation
						Operation insertOperation = createInsertOperation(queryRelNode, destTable, Collections.emptyMap(), Collections.emptyList(), false);
						Operation createTableOperation = new DDLOperationConverter(getCatalogManager()).convert(((CTASDesc) work).getCreateTableDesc());
						return Arrays.asList(createTableOperation, insertOperation);
					}
					return Collections.singletonList(new DDLOperationConverter(getCatalogManager()).convert(work));
				}
			} else {
				final boolean explain = node.getType() == HiveASTParser.TOK_EXPLAIN;
				// first child is the underlying explicandum
				ASTNode input = explain ? (ASTNode) node.getChild(0) : node;
				operation = analyzeSql(context, hiveConf, hiveShim, input);
				if (operation == null) {
					return Collections.emptyList();
				}
				if (explain) {
					operation = new ExplainOperation(operation);
				}
			}
			return Collections.singletonList(operation);
		} catch (ParseException e) {
			// ParseException can happen for flink-specific statements, e.g. catalog DDLs
			try {
				return super.parse(cmd);
			} catch (SqlParserException parserException) {
				throw new SqlParserException("SQL parse failed", e);
			}
		} catch (SemanticException | IOException e) {
			// disable fallback for now
			throw new FlinkHiveException("HiveParser failed to parse " + cmd, e);
		}
	}

	private HiveParserCalcitePlanner createCalcitePlanner(HiveParserContext context,
			HiveParserQueryState queryState, HiveShim hiveShim) throws SemanticException {
		HiveParserCalcitePlanner calciteAnalyzer = new HiveParserCalcitePlanner(
				queryState,
				plannerContext,
				catalogReader,
				frameworkConfig,
				getCatalogManager(),
				hiveShim);
		calciteAnalyzer.initCtx(context);
		calciteAnalyzer.init(false);
		return calciteAnalyzer;
	}

	private void analyzeCreateView(HiveParserCreateViewDesc desc, HiveParserContext context,
			HiveParserQueryState queryState, HiveShim hiveShim) throws SemanticException {
		HiveParserCalcitePlanner calciteAnalyzer = createCalcitePlanner(context, queryState, hiveShim);
		calciteAnalyzer.setCreateViewDesc(desc);
		calciteAnalyzer.genLogicalPlan(desc.getQuery());
	}

	private Operation analyzeSql(HiveParserContext context, HiveConf hiveConf, HiveShim hiveShim, ASTNode node)
			throws SemanticException {
		HiveParserCalcitePlanner analyzer = createCalcitePlanner(context, new HiveParserQueryState(hiveConf), hiveShim);
		RelNode relNode = analyzer.genLogicalPlan(node);
		if (relNode == null) {
			return null;
		}

		// if not a query, treat it as an insert
		if (!analyzer.getQB().getIsQuery()) {
			return createInsertOperation(analyzer, relNode);
		} else {
			return new PlannerQueryOperation(relNode);
		}
	}

	private void startSessionState(HiveConf hiveConf, CatalogManager catalogManager) {
		ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
		try {
			SessionState sessionState = new SessionState(hiveConf);
			sessionState.initTxnMgr(hiveConf);
			sessionState.setCurrentDatabase(catalogManager.getCurrentDatabase());
			// some Hive functions needs the timestamp
			sessionState.setupQueryCurrentTimestamp();
			SessionState.start(sessionState);
		} catch (LockException e) {
			throw new FlinkHiveException("Failed to init SessionState", e);
		} finally {
			// don't let SessionState mess up with our context classloader
			Thread.currentThread().setContextClassLoader(contextCL);
		}
	}

	private void clearSessionState(HiveConf hiveConf) {
		SessionState sessionState = SessionState.get();
		if (sessionState != null) {
			try {
				sessionState.close();
				List<Path> toDelete = new ArrayList<>();
				toDelete.add(SessionState.getHDFSSessionPath(hiveConf));
				toDelete.add(SessionState.getLocalSessionPath(hiveConf));
				for (Path path : toDelete) {
					FileSystem fs = path.getFileSystem(hiveConf);
					fs.delete(path, true);
				}
			} catch (IOException e) {
				LOG.warn("Error closing SessionState", e);
			}
		}
	}

	private Operation createInsertOperation(RelNode queryRelNode, Table destTable, Map<String, String> staticPartSpec,
			List<String> destSchema, boolean overwrite) throws SemanticException {
		// sanity check
		Preconditions.checkArgument(queryRelNode instanceof Project || queryRelNode instanceof Sort || queryRelNode instanceof HiveDistribution,
				"Expect top RelNode to be Project, Sort, or HiveDistribution, actually got " + queryRelNode);
		if (!(queryRelNode instanceof Project)) {
			RelNode parent = ((SingleRel) queryRelNode).getInput();
			// SEL + SORT or SEL + DIST + LIMIT
			Preconditions.checkArgument(parent instanceof Project || parent instanceof HiveDistribution,
					"Expect input to be a Project or HiveDistribution, actually got " + parent);
			if (parent instanceof HiveDistribution) {
				RelNode grandParent = ((HiveDistribution) parent).getInput();
				Preconditions.checkArgument(grandParent instanceof Project,
						"Expect input of HiveDistribution to be a Project, actually got " + grandParent);
			}
		}

		// handle dest schema, e.g. insert into dest(.,.,.) select ...
		queryRelNode = handleDestSchema((SingleRel) queryRelNode, destTable, destSchema, staticPartSpec.keySet());

		// track each target col and its expected type
		RelDataTypeFactory typeFactory = plannerContext.getTypeFactory();
		LinkedHashMap<String, RelDataType> targetColToCalcType = new LinkedHashMap<>();
		List<TypeInfo> targetHiveTypes = new ArrayList<>();
		List<FieldSchema> allCols = new ArrayList<>(destTable.getCols());
		allCols.addAll(destTable.getPartCols());
		for (FieldSchema col : allCols) {
			TypeInfo hiveType = TypeInfoUtils.getTypeInfoFromTypeString(col.getType());
			targetHiveTypes.add(hiveType);
			targetColToCalcType.put(col.getName(), HiveParserTypeConverter.convert(hiveType, typeFactory));
		}

		// add static partitions to query source
		if (!staticPartSpec.isEmpty()) {
			if (queryRelNode instanceof Project) {
				queryRelNode = replaceProjectForStaticPart((Project) queryRelNode, staticPartSpec, destTable, targetColToCalcType);
			} else if (queryRelNode instanceof Sort) {
				Sort sort = (Sort) queryRelNode;
				RelNode oldInput = sort.getInput();
				RelNode newInput;
				if (oldInput instanceof HiveDistribution) {
					newInput = replaceDistForStaticParts((HiveDistribution) oldInput, destTable, staticPartSpec, targetColToCalcType);
				} else {
					newInput = replaceProjectForStaticPart((Project) oldInput, staticPartSpec, destTable, targetColToCalcType);
					// we may need to shift the field collations
					final int numDynmPart = destTable.getTTable().getPartitionKeys().size() - staticPartSpec.size();
					if (!sort.getCollation().getFieldCollations().isEmpty() && numDynmPart > 0) {
						sort.replaceInput(0, null);
						sort = LogicalSort.create(newInput,
								shiftRelCollation(sort.getCollation(), (Project) oldInput, staticPartSpec.size(), numDynmPart),
								sort.offset, sort.fetch);
					}
				}
				sort.replaceInput(0, newInput);
				queryRelNode = sort;
			} else {
				queryRelNode = replaceDistForStaticParts((HiveDistribution) queryRelNode, destTable, staticPartSpec, targetColToCalcType);
			}
		}

		// add type conversions
		queryRelNode = addTypeConversions(plannerContext.getCluster().getRexBuilder(), queryRelNode,
				new ArrayList<>(targetColToCalcType.values()), targetHiveTypes, funcConverter);

		// create identifier
		List<String> targetTablePath = Arrays.asList(destTable.getDbName(), destTable.getTableName());
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath);
		ObjectIdentifier identifier = getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

		return new CatalogSinkModifyOperation(identifier, new PlannerQueryOperation(queryRelNode), staticPartSpec, overwrite, Collections.emptyMap());
	}

	private Operation createInsertOperation(HiveParserCalcitePlanner analyzer, RelNode queryRelNode) throws SemanticException {
		HiveParserQB topQB = analyzer.getQB();
		QBMetaData qbMetaData = topQB.getMetaData();
		// decide the dest table
		Map<String, Table> nameToDestTable = qbMetaData.getNameToDestTable();
		Map<String, Partition> nameToDestPart = qbMetaData.getNameToDestPartition();
		// for now we only support inserting to a single table
		Preconditions.checkState(nameToDestTable.size() <= 1 && nameToDestPart.size() <= 1,
				"Only support inserting to 1 table");
		Table destTable;
		String insClauseName;
		if (!nameToDestTable.isEmpty()) {
			insClauseName = nameToDestTable.keySet().iterator().next();
			destTable = nameToDestTable.values().iterator().next();
		} else if (!nameToDestPart.isEmpty()) {
			insClauseName = nameToDestPart.keySet().iterator().next();
			destTable = nameToDestPart.values().iterator().next().getTable();
		} else {
			// happens for INSERT DIRECTORY
			throw new SemanticException("INSERT DIRECTORY is not supported");
		}

		// decide static partition specs
		Map<String, String> staticPartSpec = new LinkedHashMap<>();
		if (destTable.isPartitioned()) {
			List<String> partCols = HiveCatalog.getFieldNames(destTable.getTTable().getPartitionKeys());

			if (!nameToDestPart.isEmpty()) {
				// static partition
				Partition destPart = nameToDestPart.values().iterator().next();
				Preconditions.checkState(partCols.size() == destPart.getValues().size(),
						"Part cols and static spec doesn't match");
				for (int i = 0; i < partCols.size(); i++) {
					staticPartSpec.put(partCols.get(i), destPart.getValues().get(i));
				}
			} else {
				// dynamic partition
				Map<String, String> spec = qbMetaData.getPartSpecForAlias(insClauseName);
				if (spec != null) {
					for (String partCol : partCols) {
						String val = spec.get(partCol);
						if (val != null) {
							staticPartSpec.put(partCol, val);
						}
					}
				}
			}
		}

		// decide whether it's overwrite
		boolean overwrite = topQB.getParseInfo().getInsertOverwriteTables().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet())
				.contains(destTable.getDbName() + "." + destTable.getTableName());

		return createInsertOperation(queryRelNode, destTable, staticPartSpec, analyzer.getDestSchemaForClause(insClauseName), overwrite);
	}

	private RelNode replaceDistForStaticParts(HiveDistribution hiveDist, Table destTable,
			Map<String, String> staticPartSpec, Map<String, RelDataType> targetColToType) {
		Project project = (Project) hiveDist.getInput();
		RelNode expandedProject = replaceProjectForStaticPart(project, staticPartSpec, destTable, targetColToType);
		hiveDist.replaceInput(0, null);
		final int toShift = staticPartSpec.size();
		final int numDynmPart = destTable.getTTable().getPartitionKeys().size() - toShift;
		return HiveDistribution.create(
				expandedProject,
				shiftRelCollation(hiveDist.getCollation(), project, toShift, numDynmPart),
				shiftDistKeys(hiveDist.getDistKeys(), project, toShift, numDynmPart));
	}

	private static List<Integer> shiftDistKeys(List<Integer> distKeys, Project origProject, int toShift, int numDynmPart) {
		List<Integer> shiftedDistKeys = new ArrayList<>(distKeys.size());
		// starting index of dynamic parts, static parts needs to be inserted before them
		final int insertIndex = origProject.getProjects().size() - numDynmPart;
		for (Integer key : distKeys) {
			if (key >= insertIndex) {
				key += toShift;
			}
			shiftedDistKeys.add(key);
		}
		return shiftedDistKeys;
	}

	private RelCollation shiftRelCollation(RelCollation collation, Project origProject, int toShift, int numDynmPart) {
		List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
		// starting index of dynamic parts, static parts needs to be inserted before them
		final int insertIndex = origProject.getProjects().size() - numDynmPart;
		List<RelFieldCollation> shiftedCollations = new ArrayList<>(fieldCollations.size());
		for (RelFieldCollation fieldCollation : fieldCollations) {
			if (fieldCollation.getFieldIndex() >= insertIndex) {
				fieldCollation = fieldCollation.withFieldIndex(fieldCollation.getFieldIndex() + toShift);
			}
			shiftedCollations.add(fieldCollation);
		}
		return plannerContext.getCluster().traitSet().canonize(RelCollationImpl.of(shiftedCollations));
	}

	static RelNode addTypeConversions(RexBuilder rexBuilder, RelNode queryRelNode, List<RelDataType> targetCalcTypes,
			List<TypeInfo> targetHiveTypes, ConvertSqlFunctionCopier funcConverter) throws SemanticException {
		if (queryRelNode instanceof Project) {
			return replaceProjectForTypeConversion(rexBuilder, (Project) queryRelNode, targetCalcTypes, targetHiveTypes, funcConverter);
		} else {
			// current node is not Project, we search for it in inputs
			RelNode newInput = addTypeConversions(rexBuilder, queryRelNode.getInput(0), targetCalcTypes, targetHiveTypes, funcConverter);
			queryRelNode.replaceInput(0, newInput);
			return queryRelNode;
		}
	}

	private static RexNode createConversionCast(RexBuilder rexBuilder, RexNode srcRex, TypeInfo targetHiveType,
			RelDataType targetCalType, ConvertSqlFunctionCopier funcConverter) throws SemanticException {
		if (funcConverter == null) {
			return rexBuilder.makeCast(targetCalType, srcRex);
		}
		// hive implements CAST with UDFs
		String udfName = TypeInfoUtils.getBaseName(targetHiveType.getTypeName());
		FunctionInfo functionInfo;
		try {
			functionInfo = FunctionRegistry.getFunctionInfo(udfName);
		} catch (SemanticException e) {
			throw new SemanticException(String.format("Failed to get UDF %s for casting", udfName), e);
		}
		if (functionInfo == null || functionInfo.getGenericUDF() == null) {
			throw new SemanticException(String.format("Failed to get UDF %s for casting", udfName));
		}
		if (functionInfo.getGenericUDF() instanceof SettableUDF) {
			// For SettableUDF, we need to pass target TypeInfo to it, but we don't have a way to do that currently.
			// Therefore just use calcite cast for these types.
			return rexBuilder.makeCast(targetCalType, srcRex);
		} else {
			RexCall cast = (RexCall) rexBuilder.makeCall(
					HiveParserSqlFunctionConverter.getCalciteOperator(
							udfName, functionInfo.getGenericUDF(), Collections.singletonList(srcRex.getType()), targetCalType),
					srcRex
			);
			if (!funcConverter.hasOverloadedOp(cast.getOperator(), SqlFunctionCategory.USER_DEFINED_FUNCTION)) {
				// we can't convert the cast operator, it can happen when hive module is not loaded, in which case fall
				// back to calcite cast
				return rexBuilder.makeCast(targetCalType, srcRex);
			}
			return cast.accept(funcConverter);
		}
	}

	private static RelNode replaceProjectForTypeConversion(RexBuilder rexBuilder, Project project, List<RelDataType> targetCalcTypes,
			List<TypeInfo> targetHiveTypes, ConvertSqlFunctionCopier funcConverter) throws SemanticException {
		List<RexNode> exprs = project.getProjects();
		Preconditions.checkState(exprs.size() == targetCalcTypes.size(), "Expressions and target types size mismatch");
		List<RexNode> updatedExprs = new ArrayList<>(exprs.size());
		boolean updated = false;
		for (int i = 0; i < exprs.size(); i++) {
			RexNode expr = exprs.get(i);
			if (expr.getType().getSqlTypeName() != targetCalcTypes.get(i).getSqlTypeName()) {
				TypeInfo hiveType = targetHiveTypes.get(i);
				RelDataType calcType = targetCalcTypes.get(i);
				// only support converting primitive types
				if (hiveType.getCategory() == ObjectInspector.Category.PRIMITIVE) {
					expr = createConversionCast(rexBuilder, expr, hiveType, calcType, funcConverter);
					updated = true;
				}
			}
			updatedExprs.add(expr);
		}
		if (updated) {
			RelNode newProject = LogicalProject.create(project.getInput(), Collections.emptyList(), updatedExprs,
					getProjectNames(project));
			project.replaceInput(0, null);
			return newProject;
		} else {
			return project;
		}
	}

	private RelNode handleDestSchema(SingleRel queryRelNode, Table destTable, List<String> destSchema, Set<String> staticParts)
			throws SemanticException {
		if (destSchema == null || destSchema.isEmpty()) {
			return queryRelNode;
		}

		// natural schema should contain regular cols + dynamic cols
		List<FieldSchema> naturalSchema = new ArrayList<>(destTable.getCols());
		if (destTable.isPartitioned()) {
			naturalSchema.addAll(destTable.getTTable().getPartitionKeys().stream()
					.filter(f -> !staticParts.contains(f.getName())).collect(Collectors.toList()));
		}
		// we don't need to do anything if the dest schema is the same as natural schema
		if (destSchema.equals(HiveCatalog.getFieldNames(naturalSchema))) {
			return queryRelNode;
		}
		// build a list to create a Project on top of original Project
		// for each col in dest table, if it's in dest schema, store its corresponding index in the
		// dest schema, otherwise store its type and we'll create NULL for it
		List<Object> updatedIndices = new ArrayList<>(naturalSchema.size());
		for (FieldSchema col : naturalSchema) {
			int index = destSchema.indexOf(col.getName());
			if (index < 0) {
				updatedIndices.add(HiveParserTypeConverter.convert(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()),
						plannerContext.getTypeFactory()));
			} else {
				updatedIndices.add(index);
			}
		}
		if (queryRelNode instanceof Project) {
			return addProjectForDestSchema((Project) queryRelNode, updatedIndices);
		} else if (queryRelNode instanceof Sort) {
			Sort sort = (Sort) queryRelNode;
			RelNode sortInput = sort.getInput();
			// DIST + LIMIT
			if (sortInput instanceof HiveDistribution) {
				RelNode newDist = handleDestSchemaForDist((HiveDistribution) sortInput, updatedIndices);
				sort.replaceInput(0, newDist);
				return sort;
			}
			// PROJECT + SORT
			RelNode addedProject = addProjectForDestSchema((Project) sortInput, updatedIndices);
			// we may need to update the field collations
			List<RelFieldCollation> fieldCollations = sort.getCollation().getFieldCollations();
			if (!fieldCollations.isEmpty()) {
				sort.replaceInput(0, null);
				sort = LogicalSort.create(addedProject,
						updateRelCollation(sort.getCollation(), updatedIndices),
						sort.offset, sort.fetch);
			}
			sort.replaceInput(0, addedProject);
			return sort;
		} else {
			// PROJECT + DIST
			return handleDestSchemaForDist((HiveDistribution) queryRelNode, updatedIndices);
		}
	}

	private RelNode handleDestSchemaForDist(HiveDistribution hiveDist, List<Object> updatedIndices) throws SemanticException {
		Project project = (Project) hiveDist.getInput();
		RelNode addedProject = addProjectForDestSchema(project, updatedIndices);
		// disconnect the original HiveDistribution
		hiveDist.replaceInput(0, null);
		return HiveDistribution.create(
				addedProject,
				updateRelCollation(hiveDist.getCollation(), updatedIndices),
				updateDistKeys(hiveDist.getDistKeys(), updatedIndices));
	}

	private RelCollation updateRelCollation(RelCollation collation, List<Object> updatedIndices) {
		List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
		if (fieldCollations.isEmpty()) {
			return collation;
		}
		List<RelFieldCollation> updatedCollations = new ArrayList<>(fieldCollations.size());
		for (RelFieldCollation fieldCollation : fieldCollations) {
			int newIndex = updatedIndices.indexOf(fieldCollation.getFieldIndex());
			Preconditions.checkState(newIndex >= 0, "Sort/Order references a non-existing field");
			fieldCollation = fieldCollation.withFieldIndex(newIndex);
			updatedCollations.add(fieldCollation);
		}
		return plannerContext.getCluster().traitSet().canonize(RelCollationImpl.of(updatedCollations));
	}

	private List<Integer> updateDistKeys(List<Integer> distKeys, List<Object> updatedIndices) {
		List<Integer> updatedDistKeys = new ArrayList<>(distKeys.size());
		for (Integer key : distKeys) {
			int newKey = updatedIndices.indexOf(key);
			Preconditions.checkState(newKey >= 0, "Cluster/Distribute references a non-existing field");
			updatedDistKeys.add(newKey);
		}
		return updatedDistKeys;
	}

	private RelNode replaceProjectForStaticPart(Project project, Map<String, String> staticPartSpec, Table destTable,
			Map<String, RelDataType> targetColToType) {
		List<RexNode> exprs = project.getProjects();
		List<RexNode> extendedExprs = new ArrayList<>(exprs);
		int numDynmPart = destTable.getTTable().getPartitionKeys().size() - staticPartSpec.size();
		int insertIndex = extendedExprs.size() - numDynmPart;
		RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
		for (Map.Entry<String, String> spec : staticPartSpec.entrySet()) {
			RexNode toAdd = rexBuilder.makeCharLiteral(HiveParserUtils.asUnicodeString(spec.getValue()));
			toAdd = rexBuilder.makeAbstractCast(targetColToType.get(spec.getKey()), toAdd);
			extendedExprs.add(insertIndex++, toAdd);
		}
		// TODO: we're losing the field names here, does it matter?
		RelNode res = LogicalProject.create(project.getInput(), Collections.emptyList(), extendedExprs, (List<String>) null);
		project.replaceInput(0, null);
		return res;
	}

	private static List<String> getProjectNames(Project project) {
		return project.getNamedProjects().stream().map(p -> p.right).collect(Collectors.toList());
	}

	private RelNode addProjectForDestSchema(Project input, List<Object> updatedIndices) throws SemanticException {
		int destSchemaSize = (int) updatedIndices.stream().filter(o -> o instanceof Integer).count();
		if (destSchemaSize != input.getProjects().size()) {
			throw new SemanticException(String.format("Expected %d columns, but SEL produces %d columns",
					destSchemaSize, input.getProjects().size()));
		}
		List<RexNode> exprs = new ArrayList<>(updatedIndices.size());
		RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
		for (Object object : updatedIndices) {
			if (object instanceof Integer) {
				exprs.add(rexBuilder.makeInputRef(input, (Integer) object));
			} else {
				// it's ok to call calcite to do the cast since all we cast here are nulls
				RexNode rexNode = rexBuilder.makeNullLiteral((RelDataType) object);
				exprs.add(rexNode);
			}
		}
		return LogicalProject.create(input, Collections.emptyList(), exprs, (List<String>) null);
	}
}
