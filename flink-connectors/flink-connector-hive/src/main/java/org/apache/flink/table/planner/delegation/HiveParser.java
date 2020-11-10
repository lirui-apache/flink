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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.HiveParserContext;
import org.apache.hadoop.hive.ql.HiveParserQueryState;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserTypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.FunctionSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveASTParseUtils;
import org.apache.hadoop.hive.ql.parse.HiveParserCalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParserQB;
import org.apache.hadoop.hive.ql.parse.MacroSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
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

	private final PlannerContext plannerContext;

	HiveParser(
			CatalogManager catalogManager,
			Supplier<FlinkPlannerImpl> validatorSupplier,
			Supplier<CalciteParser> calciteParserSupplier,
			Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator,
			PlannerContext plannerContext) {
		super(catalogManager, validatorSupplier, calciteParserSupplier, sqlExprToRexConverterCreator);
		this.plannerContext = plannerContext;
	}

	@Override
	public List<Operation> parse(String statement) {
		Catalog currentCatalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
		if (!(currentCatalog instanceof HiveCatalog)) {
			LOG.warn("Current catalog is not HiveCatalog. Falling back to Flink's planner.");
			return super.parse(statement);
		}
		HiveConf hiveConf = new HiveConf(((HiveCatalog) currentCatalog).getHiveConf());
		hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
		HiveShim hiveShim = HiveShimLoader.loadHiveShim(((HiveCatalog) currentCatalog).getHiveVersion());
		try {
			// creates SessionState
			SessionState sessionState = new SessionState(hiveConf);
			sessionState.initTxnMgr(hiveConf);
			sessionState.setCurrentDatabase(catalogManager.getCurrentDatabase());
			// some Hive functions needs the timestamp
			sessionState.setupQueryCurrentTimestamp();
			SessionState.start(sessionState);
			// We override Hive's grouping function. Refer to the implementation for more details.
			FunctionRegistry.registerTemporaryUDF("grouping", HiveGenericUDFGrouping.class);
			final HiveParserContext context = new HiveParserContext(hiveConf);
			// parse statement to get AST
			final ASTNode node = HiveASTParseUtils.parse(statement, context);
			final Object queryState = hiveShim.createQueryState(hiveConf);
			// generate Calcite plan
			Tuple2<BaseSemanticAnalyzer, HiveOperation> analyzerAndOperation = hiveShim.getAnalyzerAndOperation(node, hiveConf, queryState);
			Operation res;
			if (isDDL(analyzerAndOperation.f1, analyzerAndOperation.f0)) {
				return super.parse(statement);
//				res = handleDDL(node, hiveAnalyzer, context);
			} else {
				HiveParserCalcitePlanner analyzer = new HiveParserCalcitePlanner(new HiveParserQueryState(hiveConf), plannerContext, catalogManager, hiveShim);
				analyzer.initCtx(context);
				analyzer.init(false);
				RelNode relNode = analyzer.genLogicalPlan(node);
				Preconditions.checkState(relNode != null,
						String.format("%s failed to generate plan for %s", analyzer.getClass().getSimpleName(), statement));

				// if not a query, treat it as an insert
				if (!analyzer.getQB().getIsQuery()) {
					res = createInsertOperation((HiveParserCalcitePlanner) analyzer, relNode);
				} else {
					res = new PlannerQueryOperation(relNode);
				}
			}
			return res == null ? Collections.emptyList() : Collections.singletonList(res);
		} catch (ParseException e) {
			// ParseException can happen for flink-specific statements, e.g. catalog DDLs
			LOG.warn("Failed to parse SQL statement with Hive parser. Falling back to Flink's parser.", e);
			return super.parse(statement);
		} catch (SemanticException | IOException | LockException e) {
			LOG.warn("Failed to parse SQL statement with Hive parser. Falling back to Flink's parser.", e);
			// disable fallback for now
			throw new RuntimeException(e);
		} finally {
			clearSessionState(hiveConf);
		}
	}

	private static boolean isDDL(HiveOperation operation, BaseSemanticAnalyzer analyzer) {
		return analyzer instanceof DDLSemanticAnalyzer || analyzer instanceof FunctionSemanticAnalyzer ||
				analyzer instanceof MacroSemanticAnalyzer || operation == HiveOperation.CREATETABLE ||
				operation == HiveOperation.CREATETABLE_AS_SELECT || operation == HiveOperation.CREATEVIEW ||
				operation == HiveOperation.ALTERVIEW_AS;
	}

	private Operation handleDDL(ASTNode node, BaseSemanticAnalyzer hiveAnalyzer, Context context) throws SemanticException {
		hiveAnalyzer.analyze(node, context);
		DDLTask ddlTask = retrieveDDLTask(hiveAnalyzer);
		// this can happen, e.g. table already exists and "if not exists" is specified
		if (ddlTask == null) {
			return null;
		}
		DDLWork ddlWork = ddlTask.getWork();
		if (ddlWork.getCreateTblDesc() != null) {
			return handleCreateTable(ddlWork.getCreateTblDesc());
		}
		throw new UnsupportedOperationException("Unsupported DDL");
	}

	private Operation handleCreateTable(CreateTableDesc createTableDesc) throws SemanticException {
		// TODO: support NOT NULL and PK
		TableSchema tableSchema = HiveTableUtil.createTableSchema(
				createTableDesc.getCols(), createTableDesc.getPartCols(), Collections.emptySet(), null);
		List<String> partitionCols = HiveCatalog.getFieldNames(createTableDesc.getPartCols());
		// TODO: add more properties like SerDe, location, etc
		Map<String, String> tableProps = new HashMap<>(createTableDesc.getTblProps());
		tableProps.putAll(createTableDesc.getSerdeProps());
		tableProps.put(CatalogConfig.IS_GENERIC, "false");
		CatalogTable catalogTable = new CatalogTableImpl(tableSchema, partitionCols, tableProps, createTableDesc.getComment());
		String dbName = createTableDesc.getDatabaseName();
		String tabName = createTableDesc.getTableName();
		if (dbName == null || tabName.contains(".")) {
			String[] names = Utilities.getDbTableName(tabName);
			dbName = names[0];
			tabName = names[1];
		}
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(dbName, tabName);
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
		return new CreateTableOperation(identifier, catalogTable, false, false);
	}

	private DDLTask retrieveDDLTask(BaseSemanticAnalyzer hiveAnalyzer) {
		Set<DDLTask> set = new HashSet<>();
		Deque<Task<?>> queue = new ArrayDeque<>(hiveAnalyzer.getRootTasks());
		while (!queue.isEmpty()) {
			Task<?> hiveTask = queue.remove();
			if (hiveTask instanceof DDLTask) {
				set.add((DDLTask) hiveTask);
			}
			if (hiveTask.getChildTasks() != null) {
				queue.addAll(hiveTask.getChildTasks());
			}
		}
		Preconditions.checkState(set.size() <= 1, "Expect at most 1 DDLTask, actually get " + set.size());
		return set.isEmpty() ? null : set.iterator().next();
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

	private Operation createInsertOperation(HiveParserCalcitePlanner analyzer, RelNode queryRelNode) throws CalciteSemanticException {
		Preconditions.checkArgument(queryRelNode instanceof Project || queryRelNode instanceof Sort,
				"Expect top RelNode to be either Project or Sort, actually got " + queryRelNode);
		if (queryRelNode instanceof Sort) {
			RelNode sortInput = ((Sort) queryRelNode).getInput();
			Preconditions.checkArgument(sortInput instanceof Project, "" +
					"Expect input of Sort to be a Project, actually got " + sortInput);
		}
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
		} else {
			insClauseName = nameToDestPart.keySet().iterator().next();
			destTable = nameToDestPart.values().iterator().next().getTable();
		}

		// handle dest schema, e.g. insert into dest(.,.,.) select ...
		queryRelNode = handleDestSchema(queryRelNode, destTable, analyzer.getDestSchemaForClause(insClauseName));

		// create identifier
		List<String> targetTablePath = Arrays.asList(destTable.getDbName(), destTable.getTableName());
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath);
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		// track each target col and its expected type
		RelDataTypeFactory typeFactory = plannerContext.getTypeFactory();
		LinkedHashMap<String, RelDataType> targetColToType = new LinkedHashMap<>();
		List<FieldSchema> allCols = new ArrayList<>(destTable.getCols());
		allCols.addAll(destTable.getPartCols());
		for (FieldSchema col : allCols) {
			targetColToType.put(col.getName(), HiveParserTypeConverter.convert(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()), typeFactory));
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

		// add static partitions to query source
		if (!staticPartSpec.isEmpty()) {
			if (queryRelNode instanceof Project) {
				queryRelNode = replaceProjectForStaticPart((Project) queryRelNode, staticPartSpec, destTable, targetColToType);
			} else {
				Sort sort = (Sort) queryRelNode;
				Project sortInput = (Project) sort.getInput();
				RelNode expandedProject = replaceProjectForStaticPart(sortInput, staticPartSpec, destTable, targetColToType);

				List<RelFieldCollation> fieldCollations = sort.collation.getFieldCollations();
				int numDynmPart = destTable.getTTable().getPartitionKeys().size() - staticPartSpec.size();
				// we may need to shift the field collations
				if (!fieldCollations.isEmpty() && numDynmPart > 0) {
					int insertIndex = sortInput.getProjects().size() - numDynmPart;
					int toShift = staticPartSpec.size();
					List<RelFieldCollation> shiftedCollations = new ArrayList<>(fieldCollations.size());
					for (RelFieldCollation fieldCollation : fieldCollations) {
						if (fieldCollation.getFieldIndex() >= insertIndex) {
							fieldCollation = fieldCollation.withFieldIndex(fieldCollation.getFieldIndex() + toShift);
						}
						shiftedCollations.add(fieldCollation);
					}
					queryRelNode = LogicalSort.create(expandedProject,
							plannerContext.getCluster().traitSet().canonize(RelCollationImpl.of(shiftedCollations)),
							sort.offset, sort.fetch);
					sort.replaceInput(0, null);
				} else {
					queryRelNode.replaceInput(0, expandedProject);
				}
			}
		}

		// add type conversions
		queryRelNode = addTypeConversions(queryRelNode, new ArrayList<>(targetColToType.values()));

		// decide whether it's overwrite
		boolean overwrite = topQB.getParseInfo().getInsertOverwriteTables()
				.containsKey(destTable.getDbName() + "." + destTable.getTableName());

		return new CatalogSinkModifyOperation(identifier, new PlannerQueryOperation(queryRelNode), staticPartSpec, overwrite, Collections.emptyMap());
	}

	private RelNode addTypeConversions(RelNode queryRelNode, List<RelDataType> targetTypes) {
		if (queryRelNode instanceof Project) {
			return replaceProjectForTypeConversion((Project) queryRelNode, targetTypes);
		} else {
			Project sortInput = (Project) ((Sort) queryRelNode).getInput();
			RelNode newProject = replaceProjectForTypeConversion(sortInput, targetTypes);
			queryRelNode.replaceInput(0, newProject);
			return queryRelNode;
		}
	}

	private RelNode replaceProjectForTypeConversion(Project project, List<RelDataType> targetTypes) {
		// currently we only add conversion for NULL values
		List<RexNode> exprs = project.getProjects();
		Preconditions.checkState(exprs.size() == targetTypes.size(), "Expressions and target types size mismatch");
		List<RexNode> updatedExprs = new ArrayList<>(exprs.size());
		boolean updated = false;
		RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
		for (int i = 0; i < exprs.size(); i++) {
			RexNode expr = exprs.get(i);
			if (expr instanceof RexLiteral) {
				RexLiteral literal = (RexLiteral) expr;
				RelDataType targetType = targetTypes.get(i);
				if (literal.isNull() && literal.getTypeName() != targetType.getSqlTypeName()) {
					expr = rexBuilder.makeNullLiteral(targetType);
					updated = true;
				}
			}
			updatedExprs.add(expr);
		}
		if (updated) {
			RelNode newProject = LogicalProject.create(project.getInput(), Collections.emptyList(), updatedExprs,
					project.getNamedProjects().stream().map(p -> p.right).collect(Collectors.toList()));
			project.replaceInput(0, null);
			return newProject;
		} else {
			return project;
		}
	}

	private RelNode handleDestSchema(RelNode queryRelNode, Table destTable, List<String> destSchema) throws CalciteSemanticException {
		if (destSchema == null || destSchema.isEmpty()) {
			return queryRelNode;
		}
		Preconditions.checkState(!destTable.isPartitioned(), "Dest schema for partitioned table not supported yet");
		List<FieldSchema> cols = destTable.getCols();
		// we don't need to do anything if the dest schema is the same as table schema
		if (destSchema.equals(cols.stream().map(FieldSchema::getName).collect(Collectors.toList()))) {
			return queryRelNode;
		}
		// build a list to create a Project on top of query RelNode
		// for each col in dest table, if it's in dest schema, store the corresponding index of the
		// query RelNode, otherwise store its type and we'll create NULL for it
		List<Object> updatedIndices = new ArrayList<>(cols.size());
		for (FieldSchema col : cols) {
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
		} else {
			Sort sort = (Sort) queryRelNode;
			Project sortInput = (Project) sort.getInput();
			RelNode addedProject = addProjectForDestSchema(sortInput, updatedIndices);
			// we may need to update the field collations
			List<RelFieldCollation> fieldCollations = sort.collation.getFieldCollations();
			if (!fieldCollations.isEmpty()) {
				List<RelFieldCollation> updatedCollations = new ArrayList<>(fieldCollations.size());
				for (RelFieldCollation fieldCollation : fieldCollations) {
					int newIndex = updatedIndices.indexOf(fieldCollation.getFieldIndex());
					Preconditions.checkState(newIndex >= 0, "Sort references to a non-existing field");
					fieldCollation = fieldCollation.withFieldIndex(newIndex);
					updatedCollations.add(fieldCollation);
				}
				sort.replaceInput(0, null);
				sort = LogicalSort.create(addedProject,
						plannerContext.getCluster().traitSet().canonize(RelCollationImpl.of(updatedCollations)),
						sort.offset, sort.fetch);
			}
			sort.replaceInput(0, addedProject);
			return sort;
		}
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

	private RelNode addProjectForDestSchema(Project input, List<Object> updatedIndices) {
		List<RexNode> exprs = new ArrayList<>(updatedIndices.size());
		RexBuilder rexBuilder = plannerContext.getCluster().getRexBuilder();
		for (Object object : updatedIndices) {
			if (object instanceof Integer) {
				exprs.add(rexBuilder.makeInputRef(input, (Integer) object));
			} else {
				RexNode rexNode = rexBuilder.makeNullLiteral((RelDataType) object);
				exprs.add(rexNode);
			}
		}
		return LogicalProject.create(input, Collections.emptyList(), exprs, (List<String>) null);
	}
}
