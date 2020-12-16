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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.delegation.hive.ConvertSqlFunctionCopier;
import org.apache.flink.table.planner.delegation.hive.ConvertTableFunctionCopier;
import org.apache.flink.table.planner.delegation.hive.HiveParserASTBuilder;
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.table.planner.delegation.hive.HiveParserRexNodeConverter;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.table.planner.plan.nodes.hive.HiveDistribution;
import org.apache.flink.util.Preconditions;

import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.HiveParserContext;
import org.apache.hadoop.hive.ql.HiveParserQueryState;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserJoinTypeCheckCtx;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserSqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserTypeConverter;
import org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.Phase1Ctx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.generateErrorMessage;
import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.removeASTChild;
import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.rewriteGroupingFunctionAST;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getUnescapedName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getUnescapedUnqualifiedTableName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.HiveParserSemanticAnalyzer.DUMMY_TABLE;
import static org.apache.hadoop.hive.ql.parse.HiveParserSemanticAnalyzer.getColumnInternalName;

/**
 * Ported Hive's CalcitePlanner.
 */
public class HiveParserCalcitePlanner {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParserCalcitePlanner.class);

	// we hold a hive semantic analyzer instance to reuse some code
	private final HiveParserSemanticAnalyzer hiveAnalyzer;

	private final CatalogManager catalogManager;
	private final FlinkCalciteCatalogReader catalogReader;
	private final FlinkPlannerImpl flinkPlanner;
	private final PlannerContext plannerContext;
	private final FrameworkConfig frameworkConfig;

	public HiveParserCalcitePlanner(
			HiveParserQueryState queryState,
			PlannerContext plannerContext,
			FlinkCalciteCatalogReader catalogReader,
			FrameworkConfig frameworkConfig,
			CatalogManager catalogManager,
			HiveShim hiveShim) throws SemanticException {
		this.catalogManager = catalogManager;
		this.catalogReader = catalogReader;
		flinkPlanner = plannerContext.createFlinkPlanner(catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
		this.plannerContext = plannerContext;
		this.frameworkConfig = frameworkConfig;
		this.hiveAnalyzer = new HiveParserSemanticAnalyzer(queryState, hiveShim);
	}

	public void initCtx(HiveParserContext context) {
		hiveAnalyzer.initCtx(context);
	}

	public void init(boolean clearPartsCache) {
		hiveAnalyzer.init(clearPartsCache);
	}

	public HiveParserQB getQB() {
		return hiveAnalyzer.getQB();
	}

	public RelNode genLogicalPlan(ASTNode ast) throws SemanticException {
		LOG.info("Starting generating logical plan");
		HiveParserPreCboCtx cboCtx = new HiveParserPreCboCtx();
		//change the location of position alias process here
		hiveAnalyzer.processPositionAlias(ast);
		if (!hiveAnalyzer.genResolvedParseTree(ast, cboCtx)) {
			return null;
		}

		// flink requires orderBy removed from sub-queries, otherwise it can fail to generate the plan
		for (String alias : hiveAnalyzer.getQB().getSubqAliases()) {
			removeOBInSubQuery(hiveAnalyzer.getQB().getSubqForAlias(alias));
		}

		ASTNode queryForCbo = ast;
		if (cboCtx.type == HiveParserPreCboCtx.Type.CTAS || cboCtx.type == HiveParserPreCboCtx.Type.VIEW) {
			queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
		}
		if (!canCBOHandleAst(queryForCbo, getQB(), cboCtx)) {
			return null;
		}
//		profilesCBO = obtainCBOProfiles(hiveAnalyzer.getQueryProperties());
		hiveAnalyzer.disableJoinMerge = true;
		return logicalPlan();
	}

	private RelNode logicalPlan() throws SemanticException {
		FlinkHiveCalcitePlannerAction plannerAction;
		if (hiveAnalyzer.columnAccessInfo == null) {
			hiveAnalyzer.columnAccessInfo = new ColumnAccessInfo();
		}
		plannerAction = new FlinkHiveCalcitePlannerAction(hiveAnalyzer.prunedPartitions, hiveAnalyzer.columnAccessInfo);
		return plannerAction.apply(null, null, null);
	}

	private static void removeOBInSubQuery(HiveParserQBExpr qbExpr) {
		if (qbExpr == null) {
			return;
		}

		if (qbExpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
			HiveParserQB subQB = qbExpr.getQB();
			HiveParserQBParseInfo parseInfo = subQB.getParseInfo();
			String alias = qbExpr.getAlias();
			Map<String, ASTNode> destToOrderBy = parseInfo.getDestToOrderBy();
			Map<String, ASTNode> destToSortBy = parseInfo.getDestToSortBy();
			final String warning = "WARNING: Order/Sort by without limit in sub query or view [" +
					alias + "] is removed, as it's pointless and bad for performance.";
			if (destToOrderBy != null) {
				for (String dest : destToOrderBy.keySet()) {
					if (parseInfo.getDestLimit(dest) == null) {
						removeASTChild(destToOrderBy.get(dest));
						destToOrderBy.remove(dest);
						LOG.warn(warning);
					}
				}
			}
			if (destToSortBy != null) {
				for (String dest : destToSortBy.keySet()) {
					if (parseInfo.getDestLimit(dest) == null) {
						removeASTChild(destToSortBy.get(dest));
						destToSortBy.remove(dest);
						LOG.warn(warning);
					}
				}
			}
			// recursively check sub-queries
			for (String subAlias : subQB.getSubqAliases()) {
				removeOBInSubQuery(subQB.getSubqForAlias(subAlias));
			}
		} else {
			removeOBInSubQuery(qbExpr.getQBExpr1());
			removeOBInSubQuery(qbExpr.getQBExpr2());
		}
	}

	// Code responsible for Calcite plan generation and optimization.
	private class FlinkHiveCalcitePlannerAction implements Frameworks.PlannerAction<RelNode> {
		private RelOptCluster cluster;
		private RelOptSchema relOptSchema;
		private final Map<String, PrunedPartitionList> partitionCache;
		private final ColumnAccessInfo columnAccessInfo;
		private Map<Project, Table> viewProjectToTableSchema;
		private ConvertSqlFunctionCopier funcConverter;

		// correlated vars across subqueries within same query needs to have different ID
		// this will be used in HiveParserRexNodeConverter to create cor var
		private int subqueryId;

		// this is to keep track if a subquery is correlated and contains aggregate
		// since this is special cased when it is rewritten in SubqueryRemoveRule
		Set<RelNode> corrScalarRexSQWithAgg = new HashSet<>();

		// TODO: Do we need to keep track of RR, ColNameToPosMap for every op or just last one.
		LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver = new LinkedHashMap<>();
		LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap = new LinkedHashMap<>();

		FlinkHiveCalcitePlannerAction(Map<String, PrunedPartitionList> partitionCache, ColumnAccessInfo columnAccessInfo) {
			this.partitionCache = partitionCache;
			this.columnAccessInfo = columnAccessInfo;
		}

		@Override
		public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema) {
			subqueryId = 0;

			this.cluster = plannerContext.getCluster();
			this.relOptSchema = relOptSchema;
			SqlOperatorTable opTable = frameworkConfig.getOperatorTable();
			funcConverter = new ConvertSqlFunctionCopier(this.cluster.getRexBuilder(), opTable, catalogReader.nameMatcher());

			// 1. Gen Calcite Plan
			try {
				return genLogicalPlan(getQB(), true, null, null);
			} catch (SemanticException e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings("nls")
		private RelNode genSetOpLogicalPlan(HiveParserQBExpr.Opcode opcode, String alias, String leftalias, RelNode leftRel,
				String rightalias, RelNode rightRel) throws SemanticException {
			// 1. Get Row Resolvers, Column map for original left and right input of SetOp Rel
			HiveParserRowResolver leftRR = relToRowResolver.get(leftRel);
			HiveParserRowResolver rightRR = relToRowResolver.get(rightRel);
			HashMap<String, ColumnInfo> leftMap = leftRR.getFieldMap(leftalias);
			HashMap<String, ColumnInfo> rightMap = rightRR.getFieldMap(rightalias);

			// 2. Validate that SetOp is feasible according to Hive (by using type info from RR)
			if (leftMap.size() != rightMap.size()) {
				throw new SemanticException("Schema of both sides of union should match.");
			}

			// 3. construct SetOp Output RR using original left & right Input
			HiveParserRowResolver setOpOutRR = new HiveParserRowResolver();

			Iterator<Map.Entry<String, ColumnInfo>> lIter = leftMap.entrySet().iterator();
			Iterator<Map.Entry<String, ColumnInfo>> rIter = rightMap.entrySet().iterator();
			while (lIter.hasNext()) {
				Map.Entry<String, ColumnInfo> lEntry = lIter.next();
				Map.Entry<String, ColumnInfo> rEntry = rIter.next();
				ColumnInfo lInfo = lEntry.getValue();
				ColumnInfo rInfo = rEntry.getValue();

				String field = lEntry.getKey();
				// try widening conversion, otherwise fail union
				TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(), rInfo.getType());
				if (commonTypeInfo == null) {
					ASTNode tabRef = getQB().getAliases().isEmpty() ? null : getQB().getParseInfo()
							.getSrcForAlias(getQB().getAliases().get(0));
					throw new SemanticException(generateErrorMessage(tabRef,
							"Schema of both sides of setop should match: Column " + field
									+ " is of type " + lInfo.getType().getTypeName()
									+ " on first table and type " + rInfo.getType().getTypeName()
									+ " on second table"));
				}
				ColumnInfo setOpColInfo = new ColumnInfo(lInfo);
				setOpColInfo.setType(commonTypeInfo);
				setOpOutRR.put(alias, field, setOpColInfo);
			}

			// 4. Determine which columns requires cast on left/right input (Calcite requires exact types on both sides of SetOp)
			boolean leftNeedsTypeCast = false;
			boolean rightNeedsTypeCast = false;
			List<RexNode> leftProjs = new ArrayList<>();
			List<RexNode> rightProjs = new ArrayList<>();
			List<RelDataTypeField> leftFields = leftRel.getRowType().getFieldList();
			List<RelDataTypeField> rightFields = rightRel.getRowType().getFieldList();

			for (int i = 0; i < leftFields.size(); i++) {
				RelDataType leftFieldType = leftFields.get(i).getType();
				RelDataType rightFieldType = rightFields.get(i).getType();
				if (!leftFieldType.equals(rightFieldType)) {
					RelDataType unionFieldType = HiveParserUtils.toRelDataType(setOpOutRR.getColumnInfos().get(i).getType(), cluster.getTypeFactory());
					if (!unionFieldType.equals(leftFieldType)) {
						leftNeedsTypeCast = true;
					}
					leftProjs.add(cluster.getRexBuilder().ensureType(unionFieldType,
							cluster.getRexBuilder().makeInputRef(leftFieldType, i), true));

					if (!unionFieldType.equals(rightFieldType)) {
						rightNeedsTypeCast = true;
					}
					rightProjs.add(cluster.getRexBuilder().ensureType(unionFieldType,
							cluster.getRexBuilder().makeInputRef(rightFieldType, i), true));
				} else {
					leftProjs.add(cluster.getRexBuilder().ensureType(leftFieldType,
							cluster.getRexBuilder().makeInputRef(leftFieldType, i), true));
					rightProjs.add(cluster.getRexBuilder().ensureType(rightFieldType,
							cluster.getRexBuilder().makeInputRef(rightFieldType, i), true));
				}
			}

			// 5. Introduce Project Rel above original left/right inputs if cast is needed for type parity
			if (leftNeedsTypeCast) {
				leftRel = LogicalProject.create(leftRel, Collections.emptyList(), leftProjs, leftRel.getRowType().getFieldNames());
//				setOpLeftInput = HiveProject.create(leftRel, leftProjs, leftRel.getRowType().getFieldNames());
			}
			if (rightNeedsTypeCast) {
				rightRel = LogicalProject.create(rightRel, Collections.emptyList(), rightProjs, rightRel.getRowType().getFieldNames());
//				setOpRightInput = HiveProject.create(rightRel, rightProjs, rightRel.getRowType().getFieldNames());
			}

			// 6. Construct SetOp Rel
			List<RelNode> leftAndRight = Arrays.asList(leftRel, rightRel);
			SetOp setOpRel;
			switch (opcode) {
				case UNION:
					setOpRel = LogicalUnion.create(leftAndRight, true);
//					setOpRel = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), leftAndRight);
					break;
				case INTERSECT:
					setOpRel = LogicalIntersect.create(leftAndRight, false);
//					setOpRel = new HiveIntersect(cluster, TraitsUtil.getDefaultTraitSet(cluster), leftAndRight, false);
					break;
				case INTERSECTALL:
					setOpRel = LogicalIntersect.create(leftAndRight, true);
//					setOpRel = new HiveIntersect(cluster, TraitsUtil.getDefaultTraitSet(cluster), leftAndRight, true);
					break;
				case EXCEPT:
					setOpRel = LogicalMinus.create(leftAndRight, false);
//					setOpRel = new HiveExcept(cluster, TraitsUtil.getDefaultTraitSet(cluster), leftAndRight, false);
					break;
				case EXCEPTALL:
					setOpRel = LogicalMinus.create(leftAndRight, true);
//					setOpRel = new HiveExcept(cluster, TraitsUtil.getDefaultTraitSet(cluster), leftAndRight, true);
					break;
				default:
					throw new SemanticException("Unsupported set operator " + opcode.toString());
			}
			relToRowResolver.put(setOpRel, setOpOutRR);
			relToHiveColNameCalcitePosMap.put(setOpRel, buildHiveToCalciteColumnMap(setOpOutRR));
			return setOpRel;
		}

		private RelNode genJoinRelNode(RelNode leftRel, String leftTableAlias, RelNode rightRel, String rightTableAlias, JoinType hiveJoinType,
				ASTNode joinCondAst) throws SemanticException {

			HiveParserRowResolver leftRR = relToRowResolver.get(leftRel);
			HiveParserRowResolver rightRR = relToRowResolver.get(rightRel);

			// 1. Construct ExpressionNodeDesc representing Join Condition
			RexNode joinCondRex;
			List<String> namedColumns = null;
			if (joinCondAst != null) {
				HiveParserJoinTypeCheckCtx jCtx = new HiveParserJoinTypeCheckCtx(leftRR, rightRR, hiveJoinType);
				HiveParserRowResolver combinedRR = HiveParserRowResolver.getCombinedRR(leftRR, rightRR);
				// named columns join
				// TODO: we can also do the same for semi join but it seems that other DBMS does not support it yet.
				if (joinCondAst.getType() == HiveASTParser.TOK_TABCOLNAME && !hiveJoinType.equals(JoinType.LEFTSEMI)) {
					namedColumns = new ArrayList<>();
					// We will transform using clause and make it look like an on-clause.
					// So, lets generate a valid on-clause AST from using.
					ASTNode and = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.KW_AND, "and");
					ASTNode equal = null;
					int count = 0;
					for (Node child : joinCondAst.getChildren()) {
						String columnName = ((ASTNode) child).getText();
						// dealing with views
						if (hiveAnalyzer.unparseTranslator != null && hiveAnalyzer.unparseTranslator.isEnabled()) {
							hiveAnalyzer.unparseTranslator.addIdentifierTranslation((ASTNode) child);
						}
						namedColumns.add(columnName);
						ASTNode left = HiveParserASTBuilder.qualifiedName(leftTableAlias, columnName);
						ASTNode right = HiveParserASTBuilder.qualifiedName(rightTableAlias, columnName);
						equal = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.EQUAL, "=");
						HiveASTParseDriver.ADAPTOR.addChild(equal, left);
						HiveASTParseDriver.ADAPTOR.addChild(equal, right);
						HiveASTParseDriver.ADAPTOR.addChild(and, equal);
						count++;
					}
					joinCondAst = count > 1 ? and : equal;
				} else if (hiveAnalyzer.unparseTranslator != null && hiveAnalyzer.unparseTranslator.isEnabled()) {
					hiveAnalyzer.genAllExprNodeDesc(joinCondAst, combinedRR, jCtx);
				}
				Map<ASTNode, ExprNodeDesc> exprNodes = HiveParserUtils.genExprNode(joinCondAst, jCtx);
				if (jCtx.getError() != null) {
					throw new SemanticException(generateErrorMessage(jCtx.getErrorSrcNode(),
							jCtx.getError()));
				}
				ExprNodeDesc joinCondExprNode = exprNodes.get(joinCondAst);
				List<RelNode> inputRels = new ArrayList<>();
				inputRels.add(leftRel);
				inputRels.add(rightRel);
				joinCondRex = HiveParserRexNodeConverter.convert(cluster, joinCondExprNode, inputRels,
						relToRowResolver, relToHiveColNameCalcitePosMap, false).accept(funcConverter);
			} else {
				joinCondRex = cluster.getRexBuilder().makeLiteral(true);
			}

			// 2. Validate that join condition is legal (i.e no function refering to
			// both sides of join, only equi join)
			// TODO: Join filter handling (only supported for OJ by runtime or is it supported for IJ as well)

			// 3. Construct Join Rel Node and HiveParserRowResolver for the new Join Node
			boolean leftSemiJoin = false;
			JoinRelType calciteJoinType;
			switch (hiveJoinType) {
				case LEFTOUTER:
					calciteJoinType = JoinRelType.LEFT;
					break;
				case RIGHTOUTER:
					calciteJoinType = JoinRelType.RIGHT;
					break;
				case FULLOUTER:
					calciteJoinType = JoinRelType.FULL;
					break;
				case LEFTSEMI:
					calciteJoinType = JoinRelType.SEMI;
					leftSemiJoin = true;
					break;
				case INNER:
				default:
					calciteJoinType = JoinRelType.INNER;
					break;
			}

			RelNode topRel;
			HiveParserRowResolver topRR;
			if (leftSemiJoin) {
				List<RelDataTypeField> sysFieldList = new ArrayList<>();
				List<RexNode> leftJoinKeys = new ArrayList<>();
				List<RexNode> rightJoinKeys = new ArrayList<>();

				RexNode nonEquiConds = RelOptUtil.splitJoinCondition(sysFieldList, leftRel, rightRel,
						joinCondRex, leftJoinKeys, rightJoinKeys, null, null);

				if (!nonEquiConds.isAlwaysTrue()) {
					throw new SemanticException("Non equality condition not supported in Semi-Join"
							+ nonEquiConds);
				}

				RelNode[] inputRels = new RelNode[]{leftRel, rightRel};
				final List<Integer> leftKeys = new ArrayList<>();
				final List<Integer> rightKeys = new ArrayList<>();
				joinCondRex = HiveParserUtils.projectNonColumnEquiConditions(
						RelFactories.DEFAULT_PROJECT_FACTORY, inputRels, leftJoinKeys, rightJoinKeys, 0,
						leftKeys, rightKeys);
				topRel = LogicalJoin.create(inputRels[0], inputRels[1], Collections.emptyList(), joinCondRex, Collections.emptySet(), calciteJoinType);
//				topRel = HiveSemiJoin.getSemiJoin(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
//						inputRels[0], inputRels[1], joinCondRex, ImmutableIntList.copyOf(leftKeys),
//						ImmutableIntList.copyOf(rightKeys));

				// Create join RR: we need to check whether we need to update left RR in case
				// previous call to projectNonColumnEquiConditions updated it
				if (inputRels[0] != leftRel) {
					HiveParserRowResolver newLeftRR = new HiveParserRowResolver();
					if (!HiveParserRowResolver.add(newLeftRR, leftRR)) {
						LOG.warn("Duplicates detected when adding columns to RR: see previous message");
					}
					for (int i = leftRel.getRowType().getFieldCount(); i < inputRels[0].getRowType().getFieldCount(); i++) {
						ColumnInfo oColInfo = new ColumnInfo(
								getColumnInternalName(i),
								HiveParserTypeConverter.convert(inputRels[0].getRowType().getFieldList().get(i).getType()),
								null, false);
						newLeftRR.put(oColInfo.getTabAlias(), oColInfo.getInternalName(), oColInfo);
					}

					HiveParserRowResolver joinRR = new HiveParserRowResolver();
					if (!HiveParserRowResolver.add(joinRR, newLeftRR)) {
						LOG.warn("Duplicates detected when adding columns to RR: see previous message");
					}
					relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(joinRR));
					relToRowResolver.put(topRel, joinRR);

					// Introduce top project operator to remove additional column(s) that have been introduced
					List<RexNode> topFields = new ArrayList<>();
					List<String> topFieldNames = new ArrayList<>();
					for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
						final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
						topFields.add(leftRel.getCluster().getRexBuilder().makeInputRef(field.getType(), i));
						topFieldNames.add(field.getName());
					}
					topRel = LogicalProject.create(topRel, Collections.emptyList(), topFields, topFieldNames);
//					topRel = HiveRelFactories.HIVE_PROJECT_FACTORY.createProject(topRel, topFields, topFieldNames);
				}

				topRR = new HiveParserRowResolver();
				if (!HiveParserRowResolver.add(topRR, leftRR)) {
					LOG.warn("Duplicates detected when adding columns to RR: see previous message");
				}
			} else {
				topRel = LogicalJoin.create(leftRel, rightRel, Collections.emptyList(), joinCondRex, Collections.emptySet(), calciteJoinType);
//				topRel = HiveJoin.getJoin(cluster, leftRel, rightRel, joinCondRex, calciteJoinType);
				topRR = HiveParserRowResolver.getCombinedRR(leftRR, rightRR);
				if (namedColumns != null) {
					List<String> tableAliases = new ArrayList<>();
					tableAliases.add(leftTableAlias);
					tableAliases.add(rightTableAlias);
					topRR.setNamedJoinInfo(new HiveParserNamedJoinInfo(tableAliases, namedColumns, hiveJoinType));
				}
			}

			relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(topRR));
			relToRowResolver.put(topRel, topRR);
			return topRel;
		}

		// Generate Join Logical Plan Relnode by walking through the join AST.
		private RelNode genJoinLogicalPlan(ASTNode joinParseTree, Map<String, RelNode> aliasToRel)
				throws SemanticException {
			RelNode leftRel = null;
			RelNode rightRel = null;
			JoinType hiveJoinType;

			if (joinParseTree.getToken().getType() == HiveASTParser.TOK_UNIQUEJOIN) {
				String msg = "UNIQUE JOIN is currently not supported in CBO,"
						+ " turn off cbo to use UNIQUE JOIN.";
				LOG.debug(msg);
				throw new CalciteSemanticException(msg, CalciteSemanticException.UnsupportedFeature.Unique_join);
			}

			// 1. Determine Join Type
			// TODO: What about TOK_CROSSJOIN, TOK_MAPJOIN
			switch (joinParseTree.getToken().getType()) {
				case HiveASTParser.TOK_LEFTOUTERJOIN:
					hiveJoinType = JoinType.LEFTOUTER;
					break;
				case HiveASTParser.TOK_RIGHTOUTERJOIN:
					hiveJoinType = JoinType.RIGHTOUTER;
					break;
				case HiveASTParser.TOK_FULLOUTERJOIN:
					hiveJoinType = JoinType.FULLOUTER;
					break;
				case HiveASTParser.TOK_LEFTSEMIJOIN:
					hiveJoinType = JoinType.LEFTSEMI;
					break;
				default:
					hiveJoinType = JoinType.INNER;
					break;
			}

			// 2. Get Left Table Alias
			ASTNode left = (ASTNode) joinParseTree.getChild(0);
			String leftTableAlias = null;
			if (left.getToken().getType() == HiveASTParser.TOK_TABREF
					|| (left.getToken().getType() == HiveASTParser.TOK_SUBQUERY)
					|| (left.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION)) {
				String tableName = getUnescapedUnqualifiedTableName(
						(ASTNode) left.getChild(0)).toLowerCase();
				leftTableAlias = left.getChildCount() == 1 ? tableName : unescapeIdentifier(left.getChild(left.getChildCount() - 1).getText().toLowerCase());
				// ptf node form is: ^(TOK_PTBLFUNCTION $name $alias?
				// partitionTableFunctionSource partitioningSpec? expression*)
				// guranteed to have an lias here: check done in processJoin
				leftTableAlias = left.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION ?
						unescapeIdentifier(left.getChild(1).getText().toLowerCase()) :
						leftTableAlias;
				leftRel = aliasToRel.get(leftTableAlias);
			} else if (HiveParserUtils.isJoinToken(left)) {
				leftRel = genJoinLogicalPlan(left, aliasToRel);
			} else {
				assert (false);
			}

			// 3. Get Right Table Alias
			ASTNode right = (ASTNode) joinParseTree.getChild(1);
			String rightTableAlias = null;
			if (right.getToken().getType() == HiveASTParser.TOK_TABREF
					|| right.getToken().getType() == HiveASTParser.TOK_SUBQUERY
					|| right.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
				String tableName = getUnescapedUnqualifiedTableName(
						(ASTNode) right.getChild(0)).toLowerCase();
				rightTableAlias = right.getChildCount() == 1 ? tableName :
						unescapeIdentifier(right.getChild(right.getChildCount() - 1).getText().toLowerCase());
				// ptf node form is: ^(TOK_PTBLFUNCTION $name $alias?
				// partitionTableFunctionSource partitioningSpec? expression*)
				// guranteed to have an lias here: check done in processJoin
				rightTableAlias = right.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION ?
						unescapeIdentifier(right.getChild(1).getText().toLowerCase()) :
						rightTableAlias;
				rightRel = aliasToRel.get(rightTableAlias);
			} else {
				assert (false);
			}

			// 4. Get Join Condn
			ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);

			// 5. Create Join rel
			return genJoinRelNode(leftRel, leftTableAlias, rightRel, rightTableAlias, hiveJoinType, joinCond);
		}

		private RelNode genTableLogicalPlan(String tableAlias, HiveParserQB qb) throws SemanticException {
			HiveParserRowResolver rowResolver = new HiveParserRowResolver();

			try {
				// 1. If the table has a Sample specified, bail from Calcite path.
				// 2. if returnpath is on and hivetestmode is on bail
				if (qb.getParseInfo().getTabSample(tableAlias) != null
						|| hiveAnalyzer.getNameToSplitSampleMap().containsKey(tableAlias)
						|| (hiveAnalyzer.getConf().getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) && (hiveAnalyzer.getConf().getBoolVar(HiveConf.ConfVars.HIVETESTMODE))) {
					String msg = String.format("Table Sample specified for %s."
							+ " Currently we don't support Table Sample clauses in CBO,"
							+ " turn off cbo for queries on tableSamples.", tableAlias);
					LOG.debug(msg);
					throw new CalciteSemanticException(msg, CalciteSemanticException.UnsupportedFeature.Table_sample_clauses);
				}

				// 2. Get Table Metadata
				Table table = qb.getMetaData().getSrcForAlias(tableAlias);
				if (table.isTemporary()) {
					// Hive creates a temp table for VALUES, we need to convert it to LogicalValues
					RelNode values = genValues(tableAlias, table, rowResolver);
					relToRowResolver.put(values, rowResolver);
					relToHiveColNameCalcitePosMap.put(values, buildHiveToCalciteColumnMap(rowResolver));
					return values;
				} else {
					// 3. Get Table Logical Schema (Row Type)
					// NOTE: Table logical schema = Non Partition Cols + Partition Cols + Virtual Cols

					// 3.1 Add Column info for non partion cols (Object Inspector fields)
					StructObjectInspector rowObjectInspector = (StructObjectInspector) table.getDeserializer()
							.getObjectInspector();
					List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
					ColumnInfo colInfo;
					String colName;
					for (StructField field : fields) {
						colName = field.getFieldName();
						colInfo = new ColumnInfo(
								field.getFieldName(),
								TypeInfoUtils.getTypeInfoFromObjectInspector(field.getFieldObjectInspector()),
								tableAlias, false);
						colInfo.setSkewedCol(HiveParserUtils.isSkewedCol(tableAlias, qb, colName));
						rowResolver.put(tableAlias, colName, colInfo);
					}

					// 3.2 Add column info corresponding to partition columns
					for (FieldSchema partCol : table.getPartCols()) {
						colName = partCol.getName();
						colInfo = new ColumnInfo(colName,
								TypeInfoFactory.getPrimitiveTypeInfo(partCol.getType()), tableAlias, true);
						rowResolver.put(tableAlias, colName, colInfo);
					}

					final TableType tableType = obtainTableType(table);
					Preconditions.checkArgument(tableType == TableType.NATIVE, "Only native tables are supported");

					// Build Hive Table Scan Rel
					RelNode tableRel = catalogReader.getTable(Arrays.asList(catalogManager.getCurrentCatalog(),
							table.getDbName(), table.getTableName()))
							.toRel(ViewExpanders.toRelContext(flinkPlanner.createToRelContext(), cluster));

					// 6. Add Schema(RR) to RelNode-Schema map
					Map<String, Integer> hiveToCalciteColMap = buildHiveToCalciteColumnMap(rowResolver);
					relToRowResolver.put(tableRel, rowResolver);
					relToHiveColNameCalcitePosMap.put(tableRel, hiveToCalciteColMap);
					return tableRel;
				}
			} catch (Exception e) {
				if (e instanceof SemanticException) {
					throw (SemanticException) e;
				} else {
					throw (new RuntimeException(e));
				}
			}
		}

		private RelNode genValues(String tabAlias, Table tmpTable, HiveParserRowResolver rowResolver) {
			try {
				Path dataFile = new Path(tmpTable.getSd().getLocation(), "data_file");
				FileSystem fs = dataFile.getFileSystem(hiveAnalyzer.getConf());
				List<List<RexLiteral>> rows = new ArrayList<>();
				// TODO: leverage Hive to read the data
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dataFile)))) {
					QBMetaData qbMetaData = getQB().getMetaData();
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

					// non-part col types
					List<TypeInfo> hiveTargetTypes = destTable.getCols().stream()
							.map(f -> TypeInfoUtils.getTypeInfoFromTypeString(f.getType())).collect(Collectors.toList());

					// dynamic part col types
					if (destTable.isPartitioned() && nameToDestPart.isEmpty()) {
						Map<String, String> spec = qbMetaData.getPartSpecForAlias(insClauseName);
						for (FieldSchema partCol : destTable.getPartCols()) {
							if (spec.get(partCol.getName()) == null) {
								hiveTargetTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(partCol.getType()));
							}
						}
					}

					RexBuilder rexBuilder = cluster.getRexBuilder();

					// calcite target types for each field
					List<RelDataType> calciteTargetTypes = hiveTargetTypes.stream()
							.map(i -> HiveParserTypeConverter.convert((PrimitiveTypeInfo) i, rexBuilder.getTypeFactory()))
							.collect(Collectors.toList());

					// calcite field names
					List<String> calciteFieldNames = IntStream.range(0, calciteTargetTypes.size())
							.mapToObj(SqlUtil::deriveAliasFromOrdinal).collect(Collectors.toList());

					// calcite type for each row
					List<RelDataType> calciteRowTypes = new ArrayList<>();

					String line = reader.readLine();
					while (line != null) {
						String[] values = line.split("\u0001");
						List<RexLiteral> row = new ArrayList<>();
						for (int i = 0; i < hiveTargetTypes.size(); i++) {
							PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) hiveTargetTypes.get(i);
							RelDataType calciteType = calciteTargetTypes.get(i);
							if (i >= values.length || values[i].equals("\\N")) {
								row.add(rexBuilder.makeNullLiteral(calciteType));
							} else {
								String val = values[i];
								switch (primitiveTypeInfo.getPrimitiveCategory()) {
									case BYTE:
									case SHORT:
									case INT:
									case LONG:
										row.add(rexBuilder.makeExactLiteral(new BigDecimal(val), calciteType));
										break;
									case DECIMAL:
										BigDecimal bigDec = new BigDecimal(val);
										row.add(SqlTypeUtil.isValidDecimalValue(bigDec, calciteType) ?
												rexBuilder.makeExactLiteral(bigDec, calciteType) :
												rexBuilder.makeNullLiteral(calciteType));
										break;
									case FLOAT:
									case DOUBLE:
										row.add(rexBuilder.makeApproxLiteral(new BigDecimal(val), calciteType));
										break;
									case BOOLEAN:
										row.add(rexBuilder.makeLiteral(Boolean.parseBoolean(val)));
										break;
									default:
										row.add(rexBuilder.makeCharLiteral(HiveParserUtils.asUnicodeString(val)));
								}
							}
						}

						calciteRowTypes.add(rexBuilder.getTypeFactory().createStructType(
								row.stream().map(RexLiteral::getType).collect(Collectors.toList()),
								calciteFieldNames));
						rows.add(row);
						line = reader.readLine();
					}

					// compute the final row type
					RelDataType calciteRowType = rexBuilder.getTypeFactory().leastRestrictive(calciteRowTypes);
					for (int i = 0; i < calciteFieldNames.size(); i++) {
						ColumnInfo colInfo = new ColumnInfo(calciteFieldNames.get(i),
								HiveParserTypeConverter.convert(calciteRowType.getFieldList().get(i).getType()),
								tabAlias, false);
						rowResolver.put(tabAlias, calciteFieldNames.get(i), colInfo);
					}
					return HiveParserUtils.genValuesRelNode(cluster,
							rexBuilder.getTypeFactory().createStructType(calciteRowType.getFieldList()), rows);
				}
			} catch (Exception e) {
				throw new FlinkHiveException("Failed to convert temp table to LogicalValues", e);
			}
		}

		private TableType obtainTableType(Table tabMetaData) {
			if (tabMetaData.getStorageHandler() != null &&
					tabMetaData.getStorageHandler().toString().equals(
							HiveParserConstants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
				return TableType.DRUID;
			}
			return TableType.NATIVE;
		}

		private RelNode genFilterRelNode(ASTNode filterExpr, RelNode srcRel,
				Map<String, Integer> outerNameToPosMap, HiveParserRowResolver outerRR,
				boolean useCaching) throws SemanticException {
			ExprNodeDesc filterCond = hiveAnalyzer.genExprNodeDesc(filterExpr, relToRowResolver.get(srcRel), outerRR, null, useCaching);
			if (filterCond instanceof ExprNodeConstantDesc
					&& !filterCond.getTypeString().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
				// queries like select * from t1 where 'foo';
				// Calcite's rule PushFilterThroughProject chokes on it. Arguably, we
				// can insert a cast to
				// boolean in such cases, but since Postgres, Oracle and MS SQL server
				// fail on compile time
				// for such queries, its an arcane corner case, not worth of adding that
				// complexity.
				throw new CalciteSemanticException("Filter expression with non-boolean return type.",
						CalciteSemanticException.UnsupportedFeature.Filter_expression_with_non_boolean_return_type);
			}
			Map<String, Integer> hiveColNameToCalcitePos = relToHiveColNameCalcitePosMap.get(srcRel);
			RexNode convertedFilterExpr = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(),
					outerNameToPosMap, hiveColNameToCalcitePos, relToRowResolver.get(srcRel), outerRR,
					0, true, subqueryId).convert(filterCond);
			RexNode factoredFilterExpr = RexUtil.pullFactors(cluster.getRexBuilder(), convertedFilterExpr).accept(funcConverter);
			RelNode filterRel = LogicalFilter.create(srcRel, factoredFilterExpr);
//			RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
//					srcRel, factoredFilterExpr);
			relToRowResolver.put(filterRel, relToRowResolver.get(srcRel));
			relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameToCalcitePos);

			return filterRel;
		}

		private boolean topLevelConjunctCheck(ASTNode searchCond, ObjectPair<Boolean, Integer> subqInfo) {
			if (searchCond.getType() == HiveASTParser.KW_OR) {
				subqInfo.setFirst(Boolean.TRUE);
				if (subqInfo.getSecond() > 1) {
					return false;
				}
			}
			if (searchCond.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
				subqInfo.setSecond(subqInfo.getSecond() + 1);
				return subqInfo.getSecond() <= 1 || !subqInfo.getFirst();
			}
			for (int i = 0; i < searchCond.getChildCount(); i++) {
				boolean validSubQuery = topLevelConjunctCheck((ASTNode) searchCond.getChild(i), subqInfo);
				if (!validSubQuery) {
					return false;
				}
			}
			return true;
		}

		private void subqueryRestrictionCheck(HiveParserQB qb, ASTNode searchCond, RelNode srcRel,
				boolean forHavingClause,
				Set<ASTNode> corrScalarQueries) throws SemanticException {
			List<ASTNode> subQueriesInOriginalTree = HiveParserSubQueryUtils.findSubQueries(searchCond);

			ASTNode clonedSearchCond = (ASTNode) HiveParserSubQueryUtils.ADAPTOR.dupTree(searchCond);
			List<ASTNode> subQueries = HiveParserSubQueryUtils.findSubQueries(clonedSearchCond);
			for (int i = 0; i < subQueriesInOriginalTree.size(); i++) {
				//we do not care about the transformation or rewriting of AST
				// which following statement does
				// we only care about the restriction checks they perform.
				// We plan to get rid of these restrictions later
				int sqIdx = qb.incrNumSubQueryPredicates();
				ASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

				ASTNode subQueryAST = subQueries.get(i);
				// HiveParserSubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);
				ObjectPair<Boolean, Integer> subqInfo = new ObjectPair<Boolean, Integer>(false, 0);
				if (!topLevelConjunctCheck(clonedSearchCond, subqInfo)) {
					// Restriction.7.h :: SubQuery predicates can appear only as top level conjuncts.
					throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
							subQueryAST, "Only SubQuery expressions that are top level conjuncts are allowed"));

				}
				ASTNode outerQueryExpr = (ASTNode) subQueryAST.getChild(2);

				if (outerQueryExpr != null && outerQueryExpr.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {

					throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
							outerQueryExpr, "IN/NOT IN subqueries are not allowed in LHS"));
				}

				HiveParserQBSubQuery subQuery = HiveParserSubQueryUtils.buildSubQuery(qb.getId(), sqIdx, subQueryAST,
						originalSubQueryAST, hiveAnalyzer.ctx);

				HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);

				String havingInputAlias = null;

				boolean isCorrScalarWithAgg = subQuery.subqueryRestrictionsCheck(inputRR, forHavingClause, havingInputAlias);
				if (isCorrScalarWithAgg) {
					corrScalarQueries.add(originalSubQueryAST);
				}
			}
		}

		private boolean genSubQueryRelNode(HiveParserQB qb, ASTNode node, RelNode srcRel, boolean forHavingClause,
				Map<ASTNode, RelNode> subQueryToRelNode) throws SemanticException {

			Set<ASTNode> corrScalarQueriesWithAgg = new HashSet<>();
			// disallow sub-queries which HIVE doesn't currently support
			subqueryRestrictionCheck(qb, node, srcRel, forHavingClause, corrScalarQueriesWithAgg);
			Deque<ASTNode> stack = new ArrayDeque<>();
			stack.push(node);

			boolean isSubQuery = false;

			while (!stack.isEmpty()) {
				ASTNode next = stack.pop();

				switch (next.getType()) {
					case HiveASTParser.TOK_SUBQUERY_EXPR:
						// Restriction 2.h Subquery is not allowed in LHS
						if (next.getChildren().size() == 3
								&& next.getChild(2).getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
							throw new CalciteSemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
									next.getChild(2),
									"SubQuery in LHS expressions are not supported."));
						}
						String sbQueryAlias = "sq_" + qb.incrNumSubQueryPredicates();
						HiveParserQB subQB = new HiveParserQB(qb.getId(), sbQueryAlias, true);
						Phase1Ctx ctx1 = hiveAnalyzer.initPhase1Ctx();
						hiveAnalyzer.doPhase1((ASTNode) next.getChild(1), subQB, ctx1, null);
						hiveAnalyzer.getMetaData(subQB);
						RelNode subQueryRelNode = genLogicalPlan(subQB, false, relToHiveColNameCalcitePosMap.get(srcRel),
								relToRowResolver.get(srcRel));
						subQueryToRelNode.put(next, subQueryRelNode);
						//keep track of subqueries which are scalar, correlated and contains aggregate
						// subquery expression. This will later be special cased in Subquery remove rule
						if (corrScalarQueriesWithAgg.contains(next)) {
							corrScalarRexSQWithAgg.add(subQueryRelNode);
						}
						isSubQuery = true;
						break;
					default:
						int childCount = next.getChildCount();
						for (int i = childCount - 1; i >= 0; i--) {
							stack.push((ASTNode) next.getChild(i));
						}
				}
			}
			return isSubQuery;
		}

		private RelNode genFilterRelNode(HiveParserQB qb, ASTNode searchCond, RelNode srcRel,
				Map<String, Integer> outerNameToPosMap,
				HiveParserRowResolver outerRR, boolean forHavingClause) throws SemanticException {

			Map<ASTNode, RelNode> subQueryToRelNode = new HashMap<>();
			boolean isSubQuery = genSubQueryRelNode(qb, searchCond, srcRel, forHavingClause, subQueryToRelNode);
			if (isSubQuery) {
				ExprNodeDesc subQueryExpr = hiveAnalyzer.genExprNodeDesc(searchCond, relToRowResolver.get(srcRel),
						outerRR, subQueryToRelNode, forHavingClause);

				Map<String, Integer> hiveColNameToCalcitePos = relToHiveColNameCalcitePosMap.get(srcRel);
				RexNode convertedFilterLHS = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(),
						outerNameToPosMap, hiveColNameToCalcitePos, relToRowResolver.get(srcRel),
						outerRR, 0, true, subqueryId).convert(subQueryExpr).accept(funcConverter);

				RelNode filterRel = LogicalFilter.create(srcRel, convertedFilterLHS);
//				RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
//						srcRel, convertedFilterLHS);

				relToHiveColNameCalcitePosMap.put(filterRel, relToHiveColNameCalcitePosMap.get(srcRel));
				relToRowResolver.put(filterRel, relToRowResolver.get(srcRel));
				subqueryId++;
				return filterRel;
			} else {
				return genFilterRelNode(searchCond, srcRel, outerNameToPosMap, outerRR, forHavingClause);
			}
		}

		private RelNode genFilterLogicalPlan(HiveParserQB qb, RelNode srcRel,
				Map<String, Integer> outerNameToPosMap, HiveParserRowResolver outerRR,
				boolean forHavingClause) throws SemanticException {
			RelNode filterRel = null;

			Iterator<ASTNode> whereClauseIterator = getQBParseInfo(qb).getDestToWhereExpr().values().iterator();
			if (whereClauseIterator.hasNext()) {
				filterRel = genFilterRelNode(qb, (ASTNode) whereClauseIterator.next().getChild(0), srcRel,
						outerNameToPosMap, outerRR, forHavingClause);
			}

			return filterRel;
		}

		// Class to store GenericUDAF related information.
		private class AggInfo {
			private final List<ExprNodeDesc> aggParams;
			private final TypeInfo returnType;
			private final String udfName;
			private final boolean distinct;
			private final boolean isAllColumns;

			private AggInfo(List<ExprNodeDesc> aggParams, TypeInfo returnType, String udfName,
					boolean isDistinct, boolean isAllColumns) {
				this.aggParams = aggParams;
				this.returnType = returnType;
				this.udfName = udfName;
				distinct = isDistinct;
				this.isAllColumns = isAllColumns;
			}
		}

		private AggregateCall toAggCall(AggInfo aggInfo, HiveParserRexNodeConverter converter, Map<String, Integer> rexNodeToPos,
				int groupCount, RelNode input) throws SemanticException {

			// 1. Get agg fn ret type in Calcite
			RelDataType aggFnRetType = HiveParserUtils.toRelDataType(aggInfo.returnType, cluster.getTypeFactory());

			// 2. Convert Agg Fn args and type of args to Calcite
			// TODO: Does HQL allows expressions as aggregate args or can it only be projections from child?
			List<Integer> argIndices = new ArrayList<>();
			RelDataTypeFactory typeFactory = cluster.getTypeFactory();
			com.google.common.collect.ImmutableList.Builder<RelDataType> aggArgRelDTBldr =
					new com.google.common.collect.ImmutableList.Builder<>();
			for (ExprNodeDesc expr : aggInfo.aggParams) {
				RexNode paramRex = converter.convert(expr).accept(funcConverter);
				Integer argIndex = Preconditions.checkNotNull(rexNodeToPos.get(paramRex.toString()));
				argIndices.add(argIndex);

				// TODO: does arg need type cast?
				aggArgRelDTBldr.add(HiveParserUtils.toRelDataType(expr.getTypeInfo(), typeFactory));
			}

			// 3. Get Aggregation FN from Calcite given name, ret type and input arg type
			final SqlAggFunction aggFunc = HiveParserSqlFunctionConverter.getCalciteAggFn(aggInfo.udfName, aggInfo.distinct,
					aggArgRelDTBldr.build(), aggFnRetType);

			// If we have input arguments, set type to null (instead of aggFnRetType) to let AggregateCall
			// infer the type, so as to avoid nullability mismatch
			RelDataType type = null;
			if (aggInfo.isAllColumns && argIndices.isEmpty()) {
				type = aggFnRetType;
			}
			return AggregateCall.create((SqlAggFunction) funcConverter.convertOperator(aggFunc), aggInfo.distinct,
					false, false, argIndices, -1, RelCollations.EMPTY, groupCount, input, type, null);
//			return new AggregateCall(aggFunc, aggInfo.distinct, argList, aggFnRetType, null);
		}

		private RelNode genGBRelNode(List<ExprNodeDesc> gbExprs, List<AggInfo> aggInfos,
				List<Integer> groupSets, RelNode srcRel) throws SemanticException {
			Map<String, Integer> colNameToPos = relToHiveColNameCalcitePosMap.get(srcRel);
			HiveParserRexNodeConverter converter = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(), colNameToPos, 0, false);

			final boolean hasGroupSets = groupSets != null && !groupSets.isEmpty();
			final List<RexNode> gbInputRexNodes = new ArrayList<>();
			final HashMap<String, Integer> inputRexNodeToIndex = new HashMap<>();
			final List<Integer> gbKeyIndices = new ArrayList<>();
			int inputIndex = 0;
			for (ExprNodeDesc key : gbExprs) {
				// also convert null literal here to support grouping by NULLs
				RexNode keyRex = convertNullLiteral(converter.convert(key)).accept(funcConverter);
				gbInputRexNodes.add(keyRex);
				gbKeyIndices.add(inputIndex);
				inputRexNodeToIndex.put(keyRex.toString(), inputIndex);
				inputIndex++;
			}
			final ImmutableBitSet groupSet = ImmutableBitSet.of(gbKeyIndices);

			// Grouping sets: we need to transform them into ImmutableBitSet objects for Calcite
			List<ImmutableBitSet> transformedGroupSets = null;
			if (hasGroupSets) {
				Set<ImmutableBitSet> set = new HashSet<>(groupSets.size());
				for (int val : groupSets) {
					set.add(convert(val, groupSet.cardinality()));
				}
				// Calcite expects the grouping sets sorted and without duplicates
				transformedGroupSets = new ArrayList<>(set);
				transformedGroupSets.sort(ImmutableBitSet.COMPARATOR);
			}

			// add Agg parameters to inputs
			for (AggInfo aggInfo : aggInfos) {
				for (ExprNodeDesc expr : aggInfo.aggParams) {
					RexNode paramRex = converter.convert(expr).accept(funcConverter);
					Integer argIndex = inputRexNodeToIndex.get(paramRex.toString());
					if (argIndex == null) {
						argIndex = gbInputRexNodes.size();
						inputRexNodeToIndex.put(paramRex.toString(), argIndex);
						gbInputRexNodes.add(paramRex);
					}
				}
			}

			// create the actual input before creating agg calls so that the calls can properly infer return type
			RelNode gbInputRel = LogicalProject.create(srcRel, Collections.emptyList(), gbInputRexNodes, (List<String>) null);

			List<AggregateCall> aggregateCalls = new ArrayList<>();
			for (AggInfo aggInfo : aggInfos) {
				aggregateCalls.add(toAggCall(aggInfo, converter, inputRexNodeToIndex, groupSet.cardinality(), gbInputRel));
			}

			// GROUPING__ID is a virtual col in Hive, so we use Flink's function
			if (hasGroupSets) {
				// Create GroupingID column
				AggregateCall aggCall = AggregateCall.create(SqlStdOperatorTable.GROUPING_ID, false, false, false,
						gbKeyIndices, -1, RelCollations.EMPTY, groupSet.cardinality(), gbInputRel, null, null);
//				AggregateCall.create(SqlStdOperatorTable.GROUPING_ID, false, false, false,
//						groupSet.toList(), -1, RelCollations.EMPTY,
//						cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), null);
//				AggregateCall aggCall = new AggregateCall(HiveGroupingID.INSTANCE,
//						false, new ImmutableList.Builder<Integer>().build(),
//						cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
//						HiveGroupingID.INSTANCE.getName());
				aggregateCalls.add(aggCall);
			}

			if (gbInputRexNodes.isEmpty()) {
				// This will happen for count(*), in such cases we arbitrarily pick
				// first element from srcRel
				gbInputRexNodes.add(cluster.getRexBuilder().makeInputRef(srcRel, 0));
			}
//			RelNode gbInputRel = HiveProject.create(srcRel, gbChildProjLst, null);

			return LogicalAggregate.create(gbInputRel, groupSet, transformedGroupSets, aggregateCalls);
//			return new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
//					gbInputRel, (transformedGroupSets != null), groupSet,
//					transformedGroupSets, aggregateCalls);
		}

		/* This method returns the flip big-endian representation of value */
		private ImmutableBitSet convert(int value, int length) {
			BitSet bits = new BitSet();
			for (int index = length - 1; index >= 0; index--) {
				if (value % 2 != 0) {
					bits.set(index);
				}
				value = value >>> 1;
			}
			// We flip the bits because Calcite considers that '1'
			// means that the column participates in the GroupBy
			// and '0' does not, as opposed to grouping_id.
			bits.flip(0, length);
			return ImmutableBitSet.fromBitSet(bits);
		}

		private void addAlternateGByKeyMappings(ASTNode gByExpr, ColumnInfo colInfo,
				HiveParserRowResolver inputRR, HiveParserRowResolver outputRR) {
			if (gByExpr.getType() == HiveASTParser.DOT
					&& gByExpr.getChild(0).getType() == HiveASTParser.TOK_TABLE_OR_COL) {
				String tabAlias = unescapeIdentifier(gByExpr.getChild(0).getChild(0).getText().toLowerCase());
				String colAlias = unescapeIdentifier(gByExpr.getChild(1).getText().toLowerCase());
				outputRR.put(tabAlias, colAlias, colInfo);
			} else if (gByExpr.getType() == HiveASTParser.TOK_TABLE_OR_COL) {
				String colAlias = unescapeIdentifier(gByExpr.getChild(0).getText().toLowerCase());
				String tabAlias = null;
				/*
				 * If the input to the GBy has a table alias for the column, then add an entry based on that tab_alias.
				 * For e.g. this query: select b.x, count(*) from t1 b group by x needs (tab_alias=b, col_alias=x) in the
				 * GBy RR. tab_alias=b comes from looking at the HiveParserRowResolver that is the
				 * ancestor before any GBy/ReduceSinks added for the GBY operation.
				 */
				try {
					ColumnInfo pColInfo = inputRR.get(tabAlias, colAlias);
					tabAlias = pColInfo == null ? null : pColInfo.getTabAlias();
				} catch (SemanticException se) {
				}
				outputRR.put(tabAlias, colAlias, colInfo);
			}
		}

		private void addToGBExpr(HiveParserRowResolver groupByOutputRowResolver,
				HiveParserRowResolver groupByInputRowResolver, ASTNode grpbyExpr, ExprNodeDesc grpbyExprNDesc,
				List<ExprNodeDesc> gbExprNDescLst, List<String> outputColumnNames) {
			// TODO: Should we use grpbyExprNDesc.getTypeInfo()? what if expr is UDF
			int i = gbExprNDescLst.size();
			String field = getColumnInternalName(i);
			outputColumnNames.add(field);
			gbExprNDescLst.add(grpbyExprNDesc);

			ColumnInfo outColInfo = new ColumnInfo(field, grpbyExprNDesc.getTypeInfo(), null, false);
			groupByOutputRowResolver.putExpression(grpbyExpr, outColInfo);

			addAlternateGByKeyMappings(grpbyExpr, outColInfo, groupByInputRowResolver, groupByOutputRowResolver);
		}

		private AggInfo getHiveAggInfo(ASTNode aggAst, int aggFnLstArgIndx, HiveParserRowResolver inputRR,
				WindowFunctionSpec winFuncSpec) throws SemanticException {
			AggInfo aInfo;

			// 1 Convert UDAF Params to ExprNodeDesc
			ArrayList<ExprNodeDesc> aggParameters = new ArrayList<>();
			for (int i = 1; i <= aggFnLstArgIndx; i++) {
				ASTNode paraExpr = (ASTNode) aggAst.getChild(i);
				ExprNodeDesc paraExprNode = hiveAnalyzer.genExprNodeDesc(paraExpr, inputRR);
				aggParameters.add(paraExprNode);
			}

			// 2. Is this distinct UDAF
			boolean isDistinct = aggAst.getType() == HiveASTParser.TOK_FUNCTIONDI;

			// 3. Determine type of UDAF
			TypeInfo udafRetType = null;

			// 3.1 Obtain UDAF name
			String aggName = unescapeIdentifier(aggAst.getChild(0).getText());

			boolean isAllColumns = false;

			// 3.2 Rank functions type is 'int'/'double'
			if (FunctionRegistry.isRankingFunction(aggName)) {
				if (aggName.equalsIgnoreCase("percent_rank")) {
					udafRetType = TypeInfoFactory.doubleTypeInfo;
				} else {
					udafRetType = TypeInfoFactory.intTypeInfo;
				}
				// set arguments for rank functions
				for (OrderExpression orderExpr : winFuncSpec.windowSpec.getOrder().getExpressions()) {
					aggParameters.add(hiveAnalyzer.genExprNodeDesc(orderExpr.getExpression(), inputRR));
				}
			} else {
				// 3.3 Try obtaining UDAF evaluators to determine the ret type
				try {
					isAllColumns = aggAst.getType() == HiveASTParser.TOK_FUNCTIONSTAR;

					// 3.3.1 Get UDAF Evaluator
					GenericUDAFEvaluator.Mode amode = HiveParserUtils.groupByDescModeToUDAFMode(GroupByDesc.Mode.COMPLETE,
							isDistinct);

					GenericUDAFEvaluator genericUDAFEvaluator;
					if (aggName.toLowerCase().equals(FunctionRegistry.LEAD_FUNC_NAME)
							|| aggName.toLowerCase().equals(FunctionRegistry.LAG_FUNC_NAME)) {
						ArrayList<ObjectInspector> originalParameterTypeInfos = HiveParserUtils
								.getWritableObjectInspector(aggParameters);
						genericUDAFEvaluator = FunctionRegistry.getGenericWindowingEvaluator(aggName,
								originalParameterTypeInfos, isDistinct, isAllColumns);
						GenericUDAFInfo udaf = HiveParserUtils.getGenericUDAFInfo(genericUDAFEvaluator, amode,
								aggParameters);
						udafRetType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
					} else {
						genericUDAFEvaluator = HiveParserUtils.getGenericUDAFEvaluator(aggName, aggParameters,
								aggAst, isDistinct, isAllColumns);
						assert (genericUDAFEvaluator != null);

						// 3.3.2 Get UDAF Info using UDAF Evaluator
						GenericUDAFInfo udaf = HiveParserUtils.getGenericUDAFInfo(genericUDAFEvaluator, amode,
								aggParameters);
						if (FunctionRegistry.pivotResult(aggName)) {
							udafRetType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
						} else {
							udafRetType = udaf.returnType;
						}
					}
				} catch (Exception e) {
					LOG.debug("CBO: Couldn't Obtain UDAF evaluators for " + aggName
							+ ", trying to translate to GenericUDF");
				}

				// 3.4 Try GenericUDF translation
				if (udafRetType == null) {
					HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(inputRR);
					// We allow stateful functions in the SELECT list (but nowhere else)
					tcCtx.setAllowStatefulFunctions(true);
					tcCtx.setAllowDistinctFunctions(false);
					ExprNodeDesc exp = hiveAnalyzer.genExprNodeDesc((ASTNode) aggAst.getChild(0), inputRR, tcCtx);
					udafRetType = exp.getTypeInfo();
				}
			}

			// 4. Construct AggInfo
			aInfo = new AggInfo(aggParameters, udafRetType, aggName, isDistinct, isAllColumns);

			return aInfo;
		}

		// Generate GB plan.
		private RelNode genGBLogicalPlan(HiveParserQB qb, RelNode srcRel) throws SemanticException {
			RelNode gbRel = null;
			HiveParserQBParseInfo qbp = getQBParseInfo(qb);

			// 1. Gather GB Expressions (AST) (GB + Aggregations)
			// NOTE: Multi Insert is not supported
			String detsClauseName = qbp.getClauseNames().iterator().next();
			// Check and transform group by *. This will only happen for select distinct *.
			// Here the "genSelectPlan" is being leveraged.
			// The main benefits are (1) remove virtual columns that should
			// not be included in the group by; (2) add the fully qualified column names to unParseTranslator
			// so that view is supported. The drawback is that an additional SEL op is added. If it is
			// not necessary, it will be removed by NonBlockingOpDeDupProc Optimizer because it will match
			// SEL%SEL% rule.
			ASTNode selExprList = qb.getParseInfo().getSelForClause(detsClauseName);
			HiveParserSubQueryUtils.checkForTopLevelSubqueries(selExprList);
			if (selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI
					&& selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
				ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
				if (node.getToken().getType() == HiveASTParser.TOK_ALLCOLREF) {
					srcRel = genSelectLogicalPlan(qb, srcRel, srcRel, null, null);
					HiveParserRowResolver rr = relToRowResolver.get(srcRel);
					qbp.setSelExprForClause(detsClauseName, HiveParserUtils.genSelectDIAST(rr));
				}
			}

			// Select DISTINCT + windowing; GBy handled by genSelectForWindowing
			if (selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI &&
					!qb.getAllWindowingSpecs().isEmpty()) {
				return null;
			}

			List<ASTNode> gbAstExprs = hiveAnalyzer.getGroupByForClause(qbp, detsClauseName);
			HashMap<String, ASTNode> aggregationTrees = qbp.getAggregationExprsForClause(detsClauseName);
			boolean hasGrpByAstExprs = gbAstExprs != null && !gbAstExprs.isEmpty();
			boolean hasAggregationTrees = aggregationTrees != null && !aggregationTrees.isEmpty();

			final boolean cubeRollupGrpSetPresent = !qbp.getDestRollups().isEmpty()
					|| !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty();

			// 2. Sanity check
			if (hiveAnalyzer.getConf().getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
					&& qbp.getDistinctFuncExprsForClause(detsClauseName).size() > 1) {
				throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg());
			}
			if (cubeRollupGrpSetPresent) {
				if (!HiveConf.getBoolVar(hiveAnalyzer.getConf(), HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
					throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
				}

				if (hiveAnalyzer.getConf().getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
					hiveAnalyzer.checkExpressionsForGroupingSet(gbAstExprs, qb.getParseInfo()
									.getDistinctFuncExprsForClause(detsClauseName), aggregationTrees,
							this.relToRowResolver.get(srcRel));

					if (qbp.getDestGroupingSets().size() > hiveAnalyzer.getConf()
							.getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY)) {
						String errorMsg = "The number of rows per input row due to grouping sets is "
								+ qbp.getDestGroupingSets().size();
						throw new SemanticException(
								ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
					}
				}
			}

			if (hasGrpByAstExprs || hasAggregationTrees) {
				ArrayList<ExprNodeDesc> gbExprNodeDescs = new ArrayList<>();
				ArrayList<String> outputColNames = new ArrayList<>();

				// 3. Input, Output Row Resolvers
				HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
				HiveParserRowResolver outputRR = new HiveParserRowResolver();
				outputRR.setIsExprResolver(true);

				if (hasGrpByAstExprs) {
					// 4. Construct GB Keys (ExprNode)
					for (ASTNode gbAstExpr : gbAstExprs) {
						Map<ASTNode, ExprNodeDesc> astToExprNodeDesc = hiveAnalyzer.genAllExprNodeDesc(gbAstExpr, inputRR);
						ExprNodeDesc grpbyExprNDesc = astToExprNodeDesc.get(gbAstExpr);
						if (grpbyExprNDesc == null) {
							throw new CalciteSemanticException("Invalid Column Reference: " + gbAstExpr.dump(),
									CalciteSemanticException.UnsupportedFeature.Invalid_column_reference);
						}

						addToGBExpr(outputRR, inputRR, gbAstExpr, grpbyExprNDesc, gbExprNodeDescs, outputColNames);
					}
				}

				// 5. GroupingSets, Cube, Rollup
				int numGroupCols = gbExprNodeDescs.size();
				List<Integer> groupingSets = null;
				if (cubeRollupGrpSetPresent) {
					if (qbp.getDestRollups().contains(detsClauseName)) {
						groupingSets = hiveAnalyzer.getGroupingSetsForRollup(gbAstExprs.size());
					} else if (qbp.getDestCubes().contains(detsClauseName)) {
						groupingSets = hiveAnalyzer.getGroupingSetsForCube(gbAstExprs.size());
					} else if (qbp.getDestGroupingSets().contains(detsClauseName)) {
						groupingSets = hiveAnalyzer.getGroupingSets(gbAstExprs, qbp, detsClauseName);
					}

					// TODO: this seems Hive specific, need to verify how these values are produced
//					final int limit = numGroupCols * 2;
//					while (numGroupCols < limit) {
//						String field = getColumnInternalName(numGroupCols);
//						outputColNames.add(field);
// 						outputRR.put(null, field,
//								new ColumnInfo(
//										field,
//										TypeInfoFactory.booleanTypeInfo,
//										null,
//										false));
//  						numGroupCols++;
// 					}
				}

				// 6. Construct aggregation function Info
				ArrayList<AggInfo> aggInfos = new ArrayList<>();
				if (hasAggregationTrees) {
					for (ASTNode value : aggregationTrees.values()) {
						// 6.1 Determine type of UDAF
						// This is the GenericUDAF name
						String aggName = unescapeIdentifier(value.getChild(0).getText());
						boolean isDistinct = value.getType() == HiveASTParser.TOK_FUNCTIONDI;
						boolean isAllColumns = value.getType() == HiveASTParser.TOK_FUNCTIONSTAR;

						// 6.2 Convert UDAF Params to ExprNodeDesc
						ArrayList<ExprNodeDesc> aggParameters = new ArrayList<>();
						for (int i = 1; i < value.getChildCount(); i++) {
							ASTNode paraExpr = (ASTNode) value.getChild(i);
							ExprNodeDesc paraExprNode = hiveAnalyzer.genExprNodeDesc(paraExpr, inputRR);
							aggParameters.add(paraExprNode);
						}

						GenericUDAFEvaluator.Mode aggMode = HiveParserUtils.groupByDescModeToUDAFMode(
								GroupByDesc.Mode.COMPLETE, isDistinct);
						GenericUDAFEvaluator genericUDAFEvaluator = HiveParserUtils.getGenericUDAFEvaluator(
								aggName, aggParameters, value, isDistinct, isAllColumns);
						assert (genericUDAFEvaluator != null);
						GenericUDAFInfo udaf = HiveParserUtils.getGenericUDAFInfo(genericUDAFEvaluator, aggMode, aggParameters);
						AggInfo aggInfo = new AggInfo(aggParameters, udaf.returnType, aggName, isDistinct, isAllColumns);
						aggInfos.add(aggInfo);
						String field = getColumnInternalName(numGroupCols + aggInfos.size() - 1);
						outputColNames.add(field);
						outputRR.putExpression(value, new ColumnInfo(field, aggInfo.returnType, "", false));
					}
				}

				// 7. If GroupingSets, Cube, Rollup were used, we account grouping__id
				// GROUPING__ID is also required by the GROUPING function, so let's always add it for grouping sets
				if (groupingSets != null && !groupingSets.isEmpty()) {
					String field = getColumnInternalName(numGroupCols + aggInfos.size());
					outputColNames.add(field);
					outputRR.put(null, VirtualColumn.GROUPINGID.getName(),
							new ColumnInfo(
									field,
									// flink grouping_id's return type is bigint
									TypeInfoFactory.longTypeInfo,
									null,
									true));
				}

				// 8. We create the group_by operator
				gbRel = genGBRelNode(gbExprNodeDescs, aggInfos, groupingSets, srcRel);
				relToHiveColNameCalcitePosMap.put(gbRel, buildHiveToCalciteColumnMap(outputRR));
				relToRowResolver.put(gbRel, outputRR);
			}

			return gbRel;
		}

		// Generate plan for sort by, cluster by and distribute by. This is basically same as generating order by plan.
		// Should refactor to combine them.
		private Pair<RelNode, RelNode> genDistSortBy(HiveParserQB qb, RelNode srcRel, boolean outermostOB) throws SemanticException {
			RelNode res = null;
			RelNode originalInput = null;

			HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			String destClause = qbp.getClauseNames().iterator().next();

			ASTNode sortAST = qbp.getSortByForClause(destClause);
			ASTNode distAST = qbp.getDistributeByForClause(destClause);
			ASTNode clusterAST = qbp.getClusterByForClause(destClause);

			if (sortAST != null || distAST != null || clusterAST != null) {
				List<RexNode> virtualCols = new ArrayList<>();
				List<Pair<ASTNode, TypeInfo>> vcASTAndType = new ArrayList<>();
				List<RelFieldCollation> fieldCollations = new ArrayList<>();
				List<Integer> distKeys = new ArrayList<>();

				HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
				HiveParserRexNodeConverter converter = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(),
						relToHiveColNameCalcitePosMap.get(srcRel), 0, false);
				int numSrcFields = srcRel.getRowType().getFieldCount();

				// handle cluster by
				if (clusterAST != null) {
					if (sortAST != null) {
						throw new SemanticException("Cannot have both CLUSTER BY and SORT BY");
					}
					if (distAST != null) {
						throw new SemanticException("Cannot have both CLUSTER BY and DISTRIBUTE BY");
					}
					for (Node node : clusterAST.getChildren()) {
						ASTNode childAST = (ASTNode) node;
						Map<ASTNode, ExprNodeDesc> astToExprNodeDesc = hiveAnalyzer.genAllExprNodeDesc(childAST, inputRR);
						ExprNodeDesc childNodeDesc = astToExprNodeDesc.get(childAST);
						if (childNodeDesc == null) {
							throw new SemanticException("Invalid CLUSTER BY expression: " + childAST.toString());
						}
						RexNode childRexNode = converter.convert(childNodeDesc).accept(funcConverter);
						int fieldIndex;
						if (childRexNode instanceof RexInputRef) {
							fieldIndex = ((RexInputRef) childRexNode).getIndex();
						} else {
							fieldIndex = numSrcFields + virtualCols.size();
							virtualCols.add(childRexNode);
							vcASTAndType.add(new Pair<>(childAST, childNodeDesc.getTypeInfo()));
						}
						// cluster by doesn't support specifying ASC/DESC or NULLS FIRST/LAST, so use default values
						fieldCollations.add(new RelFieldCollation(
								fieldIndex,
								RelFieldCollation.Direction.ASCENDING,
								RelFieldCollation.NullDirection.FIRST));
						distKeys.add(fieldIndex);
					}
				} else {
					// handle sort by
					if (sortAST != null) {
						for (Node node : sortAST.getChildren()) {
							ASTNode childAST = (ASTNode) node;
							ASTNode nullOrderAST = (ASTNode) childAST.getChild(0);
							ASTNode fieldAST = (ASTNode) nullOrderAST.getChild(0);
							Map<ASTNode, ExprNodeDesc> astToExprNodeDesc = hiveAnalyzer.genAllExprNodeDesc(fieldAST, inputRR);
							ExprNodeDesc fieldNodeDesc = astToExprNodeDesc.get(fieldAST);
							if (fieldNodeDesc == null) {
								throw new SemanticException("Invalid sort by expression: " + fieldAST.toString());
							}
							RexNode childRexNode = converter.convert(fieldNodeDesc).accept(funcConverter);
							int fieldIndex;
							if (childRexNode instanceof RexInputRef) {
								fieldIndex = ((RexInputRef) childRexNode).getIndex();
							} else {
								fieldIndex = numSrcFields + virtualCols.size();
								virtualCols.add(childRexNode);
								vcASTAndType.add(new Pair<>(childAST, fieldNodeDesc.getTypeInfo()));
							}
							RelFieldCollation.Direction direction = RelFieldCollation.Direction.DESCENDING;
							if (childAST.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
								direction = RelFieldCollation.Direction.ASCENDING;
							}
							RelFieldCollation.NullDirection nullOrder;
							if (nullOrderAST.getType() == HiveASTParser.TOK_NULLS_FIRST) {
								nullOrder = RelFieldCollation.NullDirection.FIRST;
							} else if (nullOrderAST.getType() == HiveASTParser.TOK_NULLS_LAST) {
								nullOrder = RelFieldCollation.NullDirection.LAST;
							} else {
								throw new SemanticException("Unexpected null ordering option: " + nullOrderAST.getType());
							}
							fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullOrder));
						}
					}
					// handle distribute by
					if (distAST != null) {
						for (Node node : distAST.getChildren()) {
							ASTNode childAST = (ASTNode) node;
							Map<ASTNode, ExprNodeDesc> astToExprNodeDesc = hiveAnalyzer.genAllExprNodeDesc(childAST, inputRR);
							ExprNodeDesc childNodeDesc = astToExprNodeDesc.get(childAST);
							if (childNodeDesc == null) {
								throw new SemanticException("Invalid DISTRIBUTE BY expression: " + childAST.toString());
							}
							RexNode childRexNode = converter.convert(childNodeDesc).accept(funcConverter);
							int fieldIndex;
							if (childRexNode instanceof RexInputRef) {
								fieldIndex = ((RexInputRef) childRexNode).getIndex();
							} else {
								fieldIndex = numSrcFields + virtualCols.size();
								virtualCols.add(childRexNode);
								vcASTAndType.add(new Pair<>(childAST, childNodeDesc.getTypeInfo()));
							}
							distKeys.add(fieldIndex);
						}
					}
				}
				Preconditions.checkState(!fieldCollations.isEmpty() || !distKeys.isEmpty(),
						"Both field collations and dist keys are empty");

				// add child SEL if needed
				RelNode realInput = srcRel;
				HiveParserRowResolver outputRR = new HiveParserRowResolver();
				if (!virtualCols.isEmpty()) {
					List<RexNode> originalInputRefs = srcRel.getRowType().getFieldList().stream()
							.map(input -> new RexInputRef(input.getIndex(), input.getType()))
							.collect(Collectors.toList());
					HiveParserRowResolver addedProjectRR = new HiveParserRowResolver();
					if (!HiveParserRowResolver.add(addedProjectRR, inputRR)) {
						throw new CalciteSemanticException(
								"Duplicates detected when adding columns to RR: see previous message",
								CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
					}
					int vColPos = inputRR.getRowSchema().getSignature().size();
					for (Pair<ASTNode, TypeInfo> astTypePair : vcASTAndType) {
						addedProjectRR.putExpression(astTypePair.getKey(), new ColumnInfo(
								getColumnInternalName(vColPos), astTypePair.getValue(), null,
								false));
						vColPos++;
					}
					realInput = genSelectRelNode(CompositeList.of(originalInputRefs, virtualCols), addedProjectRR, srcRel);

					if (outermostOB) {
						if (!HiveParserRowResolver.add(outputRR, inputRR)) {
							throw new CalciteSemanticException(
									"Duplicates detected when adding columns to RR: see previous message",
									CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
						}
					} else {
						if (!HiveParserRowResolver.add(outputRR, addedProjectRR)) {
							throw new CalciteSemanticException(
									"Duplicates detected when adding columns to RR: see previous message",
									CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
						}
					}
					originalInput = srcRel;
				} else {
					if (!HiveParserRowResolver.add(outputRR, inputRR)) {
						throw new CalciteSemanticException(
								"Duplicates detected when adding columns to RR: see previous message",
								CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
					}
				}

				// create rel node
				RelTraitSet traitSet = cluster.traitSet();
				RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.of(fieldCollations));
				res = HiveDistribution.create(realInput, canonizedCollation, distKeys);

				Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
				relToRowResolver.put(res, outputRR);
				relToHiveColNameCalcitePosMap.put(res, hiveColNameCalcitePosMap);
			}

			return (new Pair<>(res, originalInput));
		}

		/**
		 * Generate OB RelNode and input Select RelNode that should be used to
		 * introduce top constraining Project. If Input select RelNode is not
		 * present then don't introduce top constraining select.
		 */
		private Pair<Sort, RelNode> genOBLogicalPlan(HiveParserQB qb, RelNode srcRel, boolean outermostOB)
				throws SemanticException {
			Sort sortRel = null;
			RelNode originalOBInput = null;

			HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			String dest = qbp.getClauseNames().iterator().next();
			ASTNode obAST = qbp.getOrderByForClause(dest);

			if (obAST != null) {
				// 1. OB Expr sanity test
				// in strict mode, in the presence of order by, limit must be specified
				Integer limit = qb.getParseInfo().getDestLimit(dest);
				if (limit == null) {
					String mapRedMode = hiveAnalyzer.getConf().getVar(HiveConf.ConfVars.HIVEMAPREDMODE);
					boolean banLargeQuery = Boolean.parseBoolean(hiveAnalyzer.getConf().get("hive.strict.checks.large.query", "false"));
					if ("strict".equalsIgnoreCase(mapRedMode) || banLargeQuery) {
						throw new SemanticException(generateErrorMessage(obAST, "Order by-s without limit"));
					}
				}

				// 2. Walk through OB exprs and extract field collations and additional
				// virtual columns needed
				final List<RexNode> virtualCols = new ArrayList<>();
				final List<RelFieldCollation> fieldCollations = new ArrayList<>();
				int fieldIndex;

				List<Node> obASTExprLst = obAST.getChildren();
				ASTNode obASTExpr;
				ASTNode nullOrderASTExpr;
				List<Pair<ASTNode, TypeInfo>> vcASTAndType = new ArrayList<>();
				HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
				HiveParserRowResolver outputRR = new HiveParserRowResolver();

				HiveParserRexNodeConverter converter = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(),
						relToHiveColNameCalcitePosMap.get(srcRel), 0, false);
				int numSrcFields = srcRel.getRowType().getFieldCount();

				for (Node node : obASTExprLst) {
					// 2.1 Convert AST Expr to ExprNode
					obASTExpr = (ASTNode) node;
					nullOrderASTExpr = (ASTNode) obASTExpr.getChild(0);
					ASTNode ref = (ASTNode) nullOrderASTExpr.getChild(0);
					Map<ASTNode, ExprNodeDesc> astToExprNodeDesc = hiveAnalyzer.genAllExprNodeDesc(ref, inputRR);
					ExprNodeDesc obExprNodeDesc = astToExprNodeDesc.get(ref);
					if (obExprNodeDesc == null) {
						throw new SemanticException("Invalid order by expression: " + obASTExpr.toString());
					}

					// 2.2 Convert ExprNode to RexNode
					RexNode rexNode = converter.convert(obExprNodeDesc).accept(funcConverter);

					// 2.3 Determine the index of ob expr in child schema
					// NOTE: Calcite can not take compound exprs in OB without it being
					// present in the child (& hence we add a child Project Rel)
					if (rexNode instanceof RexInputRef) {
						fieldIndex = ((RexInputRef) rexNode).getIndex();
					} else {
						fieldIndex = numSrcFields + virtualCols.size();
						virtualCols.add(rexNode);
						vcASTAndType.add(new Pair<>(ref, obExprNodeDesc.getTypeInfo()));
					}

					// 2.4 Determine the Direction of order by
					RelFieldCollation.Direction direction = RelFieldCollation.Direction.DESCENDING;
					if (obASTExpr.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
						direction = RelFieldCollation.Direction.ASCENDING;
					}
					RelFieldCollation.NullDirection nullOrder;
					if (nullOrderASTExpr.getType() == HiveASTParser.TOK_NULLS_FIRST) {
						nullOrder = RelFieldCollation.NullDirection.FIRST;
					} else if (nullOrderASTExpr.getType() == HiveASTParser.TOK_NULLS_LAST) {
						nullOrder = RelFieldCollation.NullDirection.LAST;
					} else {
						throw new SemanticException("Unexpected null ordering option: " + nullOrderASTExpr.getType());
					}

					// 2.5 Add to field collations
					fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullOrder));
				}

				// 3. Add Child Project Rel if needed, Generate Output RR, input Sel Rel
				// for top constraining Sel
				RelNode obInputRel = srcRel;
				if (!virtualCols.isEmpty()) {
					List<RexNode> originalInputRefs = srcRel.getRowType().getFieldList().stream()
							.map(input -> new RexInputRef(input.getIndex(), input.getType()))
							.collect(Collectors.toList());
					HiveParserRowResolver obSyntheticProjectRR = new HiveParserRowResolver();
					if (!HiveParserRowResolver.add(obSyntheticProjectRR, inputRR)) {
						throw new CalciteSemanticException(
								"Duplicates detected when adding columns to RR: see previous message",
								CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
					}
					int vcolPos = inputRR.getRowSchema().getSignature().size();
					for (Pair<ASTNode, TypeInfo> astTypePair : vcASTAndType) {
						obSyntheticProjectRR.putExpression(astTypePair.getKey(), new ColumnInfo(
								getColumnInternalName(vcolPos), astTypePair.getValue(), null,
								false));
						vcolPos++;
					}
					obInputRel = genSelectRelNode(CompositeList.of(originalInputRefs, virtualCols),
							obSyntheticProjectRR, srcRel);

					if (outermostOB) {
						if (!HiveParserRowResolver.add(outputRR, inputRR)) {
							throw new CalciteSemanticException(
									"Duplicates detected when adding columns to RR: see previous message",
									CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
						}
					} else {
						if (!HiveParserRowResolver.add(outputRR, obSyntheticProjectRR)) {
							throw new CalciteSemanticException(
									"Duplicates detected when adding columns to RR: see previous message",
									CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
						}
					}
					originalOBInput = srcRel;
				} else {
					if (!HiveParserRowResolver.add(outputRR, inputRR)) {
						throw new CalciteSemanticException(
								"Duplicates detected when adding columns to RR: see previous message",
								CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
					}
				}

				// 4. Construct SortRel
				RelTraitSet traitSet = cluster.traitSet();
				RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.of(fieldCollations));
				sortRel = LogicalSort.create(obInputRel, canonizedCollation, null, null);
//				sortRel = new HiveSortLimit(cluster, traitSet, obInputRel, canonizedCollation, null, null);

				// 5. Update the maps
				// NOTE: Output RR for SortRel is considered same as its input; we may
				// end up not using VC that is present in sort rel. Also note that
				// rowtype of sortrel is the type of it child; if child happens to be
				// synthetic project that we introduced then that projectrel would
				// contain the vc.
				Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
				relToRowResolver.put(sortRel, outputRR);
				relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
			}

			return (new Pair<>(sortRel, originalOBInput));
		}

		private Sort genLimitLogicalPlan(HiveParserQB qb, RelNode srcRel) throws SemanticException {
			Sort sortRel = null;
			HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			AbstractMap.SimpleEntry<Integer, Integer> entry =
					qbp.getDestToLimit().get(qbp.getClauseNames().iterator().next());
			Integer offset = (entry == null) ? 0 : entry.getKey();
			Integer fetch = (entry == null) ? null : entry.getValue();

			if (fetch != null) {
				RexNode offsetRex = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offset));
				RexNode fetchRex = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(fetch));
				RelTraitSet traitSet = cluster.traitSet();
				RelCollation canonizedCollation = traitSet.canonize(RelCollations.EMPTY);
				sortRel = LogicalSort.create(srcRel, canonizedCollation, offsetRex, fetchRex);
//				sortRel = new HiveSortLimit(cluster, traitSet, srcRel, canonizedCollation, offsetRN, fetchRN);

				HiveParserRowResolver outputRR = new HiveParserRowResolver();
				if (!HiveParserRowResolver.add(outputRR, relToRowResolver.get(srcRel))) {
					throw new CalciteSemanticException(
							"Duplicates detected when adding columns to RR: see previous message",
							CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
				}
				Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
				relToRowResolver.put(sortRel, outputRR);
				relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
			}

			return sortRel;
		}

		private List<RexNode> getPartitionKeys(PartitionSpec partitionSpec, HiveParserRexNodeConverter converter,
				HiveParserRowResolver inputRR) throws SemanticException {
			List<RexNode> res = new ArrayList<>();
			if (partitionSpec != null) {
				List<PartitionExpression> expressions = partitionSpec.getExpressions();
				for (PartitionExpression expression : expressions) {
					HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(inputRR);
					typeCheckCtx.setAllowStatefulFunctions(true);
					ExprNodeDesc exp = hiveAnalyzer.genExprNodeDesc(expression.getExpression(), inputRR, typeCheckCtx);
					res.add(converter.convert(exp));
				}
			}

			return res;
		}

		private List<RexFieldCollation> getOrderKeys(HiveParserPTFInvocationSpec.OrderSpec orderSpec, HiveParserRexNodeConverter converter,
				HiveParserRowResolver inputRR) throws SemanticException {
			List<RexFieldCollation> orderKeys = new ArrayList<>();
			if (orderSpec != null) {
				List<HiveParserPTFInvocationSpec.OrderExpression> oExprs = orderSpec.getExpressions();
				for (HiveParserPTFInvocationSpec.OrderExpression oExpr : oExprs) {
					HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(inputRR);
					tcCtx.setAllowStatefulFunctions(true);
					ExprNodeDesc exp = hiveAnalyzer.genExprNodeDesc(oExpr.getExpression(), inputRR, tcCtx);
					RexNode ordExp = converter.convert(exp);
					Set<SqlKind> flags = new HashSet<>();
					if (oExpr.getOrder() == Order.DESC) {
						flags.add(SqlKind.DESCENDING);
					}
					if (oExpr.getNullOrder() == org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.NullOrder.NULLS_FIRST) {
						flags.add(SqlKind.NULLS_FIRST);
					} else if (oExpr.getNullOrder() == org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.NullOrder.NULLS_LAST) {
						flags.add(SqlKind.NULLS_LAST);
					} else {
						throw new SemanticException(
								"Unexpected null ordering option: " + oExpr.getNullOrder());
					}
					orderKeys.add(new RexFieldCollation(ordExp, flags));
				}
			}

			return orderKeys;
		}

		private RexWindowBound getBound(BoundarySpec spec) {
			RexWindowBound res = null;

			if (spec != null) {
				SqlParserPos dummyPos = new SqlParserPos(1, 1);
				SqlNode amt = spec.getAmt() == 0 || spec.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT ? null :
						SqlLiteral.createExactNumeric(String.valueOf(spec.getAmt()), new SqlParserPos(2, 2));
				RexNode amtLiteral = amt == null ? null :
						cluster.getRexBuilder().makeLiteral(spec.getAmt(),
								cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);

				switch (spec.getDirection()) {
					case PRECEDING:
						if (amt == null) {
							res = RexWindowBound.create(SqlWindow.createUnboundedPreceding(dummyPos), null);
						} else {
							SqlCall call = (SqlCall) SqlWindow.createPreceding(amt, dummyPos);
							res = RexWindowBound.create(call,
									cluster.getRexBuilder().makeCall(call.getOperator(), amtLiteral));
						}
						break;

					case CURRENT:
						res = RexWindowBound.create(SqlWindow.createCurrentRow(dummyPos), null);
						break;

					case FOLLOWING:
						if (amt == null) {
							res = RexWindowBound.create(SqlWindow.createUnboundedFollowing(dummyPos), null);
						} else {
							SqlCall call = (SqlCall) SqlWindow.createFollowing(amt, dummyPos);
							res = RexWindowBound.create(call,
									cluster.getRexBuilder().makeCall(call.getOperator(), amtLiteral));
						}
						break;
				}
			}

			return res;
		}

		private int getWindowSpecIndx(ASTNode wndAST) {
			int wi = wndAST.getChildCount() - 1;
			if (wi <= 0 || (wndAST.getChild(wi).getType() != HiveASTParser.TOK_WINDOWSPEC)) {
				wi = -1;
			}

			return wi;
		}

		private Pair<RexNode, TypeInfo> getWindowRexAndType(WindowExpressionSpec winExprSpec, RelNode srcRel)
				throws SemanticException {
			RexNode window;

			if (winExprSpec instanceof WindowFunctionSpec) {
				WindowFunctionSpec wFnSpec = (WindowFunctionSpec) winExprSpec;
				ASTNode windowProjAst = wFnSpec.getExpression();
				// TODO: do we need to get to child?
				int wndSpecASTIndx = getWindowSpecIndx(windowProjAst);
				// 2. Get Hive Aggregate Info
				AggInfo hiveAggInfo = getHiveAggInfo(windowProjAst, wndSpecASTIndx - 1,
						relToRowResolver.get(srcRel), (WindowFunctionSpec) winExprSpec);

				// 3. Get Calcite Return type for Agg Fn
				RelDataType calciteAggFnRetType = HiveParserUtils.toRelDataType(hiveAggInfo.returnType, cluster.getTypeFactory());

				// 4. Convert Agg Fn args to Calcite
				Map<String, Integer> posMap = relToHiveColNameCalcitePosMap.get(srcRel);
				HiveParserRexNodeConverter converter = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(), posMap, 0, false);
				com.google.common.collect.ImmutableList.Builder<RexNode> calciteAggFnArgsBldr =
						com.google.common.collect.ImmutableList.builder();
				com.google.common.collect.ImmutableList.Builder<RelDataType> calciteAggFnArgsTypeBldr =
						com.google.common.collect.ImmutableList.builder();
				for (int i = 0; i < hiveAggInfo.aggParams.size(); i++) {
					calciteAggFnArgsBldr.add(converter.convert(hiveAggInfo.aggParams.get(i)));
					calciteAggFnArgsTypeBldr.add(HiveParserUtils.toRelDataType(hiveAggInfo.aggParams.get(i).getTypeInfo(),
							cluster.getTypeFactory()));
				}
				com.google.common.collect.ImmutableList<RexNode> calciteAggFnArgs = calciteAggFnArgsBldr.build();
				com.google.common.collect.ImmutableList<RelDataType> calciteAggFnArgTypes = calciteAggFnArgsTypeBldr.build();

				// 5. Get Calcite Agg Fn
				final SqlAggFunction calciteAggFn = HiveParserSqlFunctionConverter.getCalciteAggFn(
						hiveAggInfo.udfName, hiveAggInfo.distinct, calciteAggFnArgTypes, calciteAggFnRetType);

				// 6. Translate Window spec
				HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
				WindowSpec wndSpec = ((WindowFunctionSpec) winExprSpec).getWindowSpec();
				List<RexNode> partitionKeys = getPartitionKeys(wndSpec.getPartition(), converter, inputRR);
				List<RexFieldCollation> orderKeys = getOrderKeys(wndSpec.getOrder(), converter, inputRR);
				RexWindowBound lowerBound = getBound(wndSpec.getWindowFrame().getStart());
				RexWindowBound upperBound = getBound(wndSpec.getWindowFrame().getEnd());
				boolean isRows = wndSpec.getWindowFrame().getWindowType() == WindowType.ROWS;

				// TODO: Guava is relocated in blink-planner. Therefore the following call would fail.
//				window = cluster.getRexBuilder().makeOver(calciteAggFnRetType, calciteAggFn, calciteAggFnArgs, partitionKeys,
//						ImmutableList.copyOf(orderKeys),
//						lowerBound, upperBound, isRows, true, false, false, false);
				window = HiveParserUtils.makeOver(cluster.getRexBuilder(), calciteAggFnRetType, calciteAggFn, calciteAggFnArgs,
						partitionKeys, orderKeys, lowerBound, upperBound, isRows, true, false, false, false);
				window = window.accept(funcConverter);
//				window = null;
			} else {
				// TODO: Convert to Semantic Exception
				throw new RuntimeException("Unsupported window Spec");
			}

			return new Pair<>(window, HiveParserTypeConverter.convert(window.getType()));
		}

		private RelNode genSelectForWindowing(HiveParserQB qb, RelNode srcRel, HashSet<ColumnInfo> newColumns)
				throws SemanticException {
			HiveParserWindowingSpec wSpec = !qb.getAllWindowingSpecs().isEmpty() ?
					qb.getAllWindowingSpecs().values().iterator().next() : null;
			if (wSpec == null) {
				return null;
			}
			// 1. Get valid Window Function Spec
			wSpec.validateAndMakeEffective();
			List<WindowExpressionSpec> windowExpressions = wSpec.getWindowExpressions();
			if (windowExpressions == null || windowExpressions.isEmpty()) {
				return null;
			}

			HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
			// 2. Get RexNodes for original Projections from below
			List<RexNode> projsForWindowSelOp = new ArrayList<>(HiveCalciteUtil.getProjsFromBelowAsInputRef(srcRel));

			// 3. Construct new Row Resolver with everything from below.
			HiveParserRowResolver outRR = new HiveParserRowResolver();
			if (!HiveParserRowResolver.add(outRR, inputRR)) {
				LOG.warn("Duplicates detected when adding columns to RR: see previous message");
			}

			// 4. Walk through Window Expressions & Construct RexNodes for those. Update out_rwsch
			final HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			final String selClauseName = qbp.getClauseNames().iterator().next();
			final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
					|| !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());
			for (WindowExpressionSpec winExprSpec : windowExpressions) {
				if (!qbp.getDestToGroupBy().isEmpty()) {
					// Special handling of grouping function
					winExprSpec.setExpression(rewriteGroupingFunctionAST(
							hiveAnalyzer.getGroupByForClause(qbp, selClauseName), winExprSpec.getExpression(),
							!cubeRollupGrpSetPresent));
				}
				if (outRR.getExpression(winExprSpec.getExpression()) == null) {
					Pair<RexNode, TypeInfo> rexAndType = getWindowRexAndType(winExprSpec, srcRel);
					projsForWindowSelOp.add(rexAndType.getKey());

					// 6.2.2 Update Output Row Schema
					ColumnInfo oColInfo = new ColumnInfo(
							getColumnInternalName(projsForWindowSelOp.size()), rexAndType.getValue(),
							null, false);
					outRR.putExpression(winExprSpec.getExpression(), oColInfo);
					newColumns.add(oColInfo);
				}
			}

			return genSelectRelNode(projsForWindowSelOp, outRR, srcRel, windowExpressions);
		}

		private RelNode genSelectRelNode(List<RexNode> calciteColLst, HiveParserRowResolver outRR, RelNode srcRel) {
			return genSelectRelNode(calciteColLst, outRR, srcRel, null);
		}

		private RelNode genSelectRelNode(List<RexNode> calciteColLst, HiveParserRowResolver outRR,
				RelNode srcRel, List<WindowExpressionSpec> windowExpressions) {
			// 1. Build Column Names
			Set<String> colNames = new HashSet<>();
			List<ColumnInfo> colInfos = outRR.getRowSchema().getSignature();
			ArrayList<String> columnNames = new ArrayList<>();
			Map<String, String> windowToAlias = null;
			if (windowExpressions != null) {
				windowToAlias = new HashMap<>();
				for (WindowExpressionSpec wes : windowExpressions) {
					windowToAlias.put(wes.getExpression().toStringTree().toLowerCase(), wes.getAlias());
				}
			}
			String[] qualifiedColNames;
			String tmpColAlias;
			for (int i = 0; i < calciteColLst.size(); i++) {
				ColumnInfo cInfo = colInfos.get(i);
				qualifiedColNames = outRR.reverseLookup(cInfo.getInternalName());
				/*
				 * if (qualifiedColNames[0] != null && !qualifiedColNames[0].isEmpty())
				 * tmpColAlias = qualifiedColNames[0] + "." + qualifiedColNames[1]; else
				 */
				tmpColAlias = qualifiedColNames[1];

				if (tmpColAlias.contains(".") || tmpColAlias.contains(":")) {
					tmpColAlias = cInfo.getInternalName();
				}
				// Prepend column names with '_o_' if it starts with '_c'
				/*
				 * Hive treats names that start with '_c' as internalNames; so change
				 * the names so we don't run into this issue when converting back to
				 * Hive AST.
				 */
				if (tmpColAlias.startsWith("_c")) {
					tmpColAlias = "_o_" + tmpColAlias;
				} else if (windowToAlias != null && windowToAlias.containsKey(tmpColAlias)) {
					tmpColAlias = windowToAlias.get(tmpColAlias);
				}
				int suffix = 1;
				while (colNames.contains(tmpColAlias)) {
					tmpColAlias = qualifiedColNames[1] + suffix;
					suffix++;
				}

				colNames.add(tmpColAlias);
				columnNames.add(tmpColAlias);
			}

			// 3 Build Calcite Rel Node for project using converted projections & col names
			RelNode selRel = LogicalProject.create(srcRel, Collections.emptyList(), calciteColLst, columnNames);

			// 4. Keep track of col name-to-pos map && RR for new select
			relToHiveColNameCalcitePosMap.put(selRel, buildHiveToCalciteColumnMap(outRR));
			relToRowResolver.put(selRel, outRR);

			return selRel;
		}

		/**
		 * NOTE: there can only be one select clause since we don't handle multi destination insert.
		 */
		private RelNode genSelectLogicalPlan(HiveParserQB qb, RelNode srcRel, RelNode starSrcRel,
				Map<String, Integer> outerNameToPos, HiveParserRowResolver outerRR)
				throws SemanticException {
			// 0. Generate a Select Node for Windowing
			// Exclude the newly-generated select columns from */etc. resolution.
			HashSet<ColumnInfo> excludedColumns = new HashSet<>();
			RelNode selForWindow = genSelectForWindowing(qb, srcRel, excludedColumns);
			srcRel = (selForWindow == null) ? srcRel : selForWindow;

			ArrayList<ExprNodeDesc> exprNodeDescs = new ArrayList<>();

			// 1. Get Select Expression List
			HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			String selClauseName = qbp.getClauseNames().iterator().next();
			ASTNode selExprList = qbp.getSelForClause(selClauseName);

			// make sure if there is subquery it is top level expression
			HiveParserSubQueryUtils.checkForTopLevelSubqueries(selExprList);

			final boolean cubeRollupGrpSetPresent = !qbp.getDestRollups().isEmpty()
					|| !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty();

			// 3. Query Hints
			// TODO: Handle Query Hints; currently we ignore them
			int posn = 0;
			boolean hintPresent = selExprList.getChild(0).getType() == HiveASTParser.QUERY_HINT;
			if (hintPresent) {
				posn++;
			}

			// 4. Bailout if select involves Transform
			boolean isInTransform = selExprList.getChild(posn).getChild(0).getType() == HiveASTParser.TOK_TRANSFORM;
			if (isInTransform) {
				String msg = "SELECT TRANSFORM is currently not supported in CBO,"
						+ " turn off cbo to use TRANSFORM.";
				LOG.debug(msg);
				throw new CalciteSemanticException(msg, CalciteSemanticException.UnsupportedFeature.Select_transform);
			}

			// 2.Row resolvers for input, output
			HiveParserRowResolver outRR = new HiveParserRowResolver();
			Integer pos = 0;
			// TODO: will this also fix windowing? try
			HiveParserRowResolver inputRR = relToRowResolver.get(srcRel), starRR = inputRR;
			if (starSrcRel != null) {
				starRR = relToRowResolver.get(starSrcRel);
			}

			// 5. Check if select involves UDTF
			String udtfTableAlias = null;
			GenericUDTF genericUDTF = null;
			String genericUDTFName = null;
			ArrayList<String> udtfColAliases = new ArrayList<>();
			ASTNode expr = (ASTNode) selExprList.getChild(posn).getChild(0);
			int exprType = expr.getType();
			if (exprType == HiveASTParser.TOK_FUNCTION || exprType == HiveASTParser.TOK_FUNCTIONSTAR) {
				String funcName = HiveParserTypeCheckProcFactory.DefaultExprProcessor.getFunctionText(expr, true);
				FunctionInfo fi = HiveParserUtils.getFunctionInfo(funcName);
				if (fi != null && fi.getGenericUDTF() != null) {
					LOG.debug("Found UDTF " + funcName);
					genericUDTF = fi.getGenericUDTF();
					genericUDTFName = funcName;
					if (!fi.isNative()) {
						hiveAnalyzer.unparseTranslator.addIdentifierTranslation((ASTNode) expr.getChild(0));
					}
					if (exprType == HiveASTParser.TOK_FUNCTIONSTAR) {
						hiveAnalyzer.genColListRegex(".*", null, (ASTNode) expr.getChild(0),
								exprNodeDescs, null, inputRR, starRR, pos, outRR, qb.getAliases(), false);
					}
				}
			}

			if (genericUDTF != null) {
				// Only support a single expression when it's a UDTF
				if (selExprList.getChildCount() > 1) {
					throw new SemanticException(generateErrorMessage(
							(ASTNode) selExprList.getChild(1),
							ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
				}

				ASTNode selExpr = (ASTNode) selExprList.getChild(posn);

				// Get the column / table aliases from the expression. Start from 1 as
				// 0 is the TOK_FUNCTION
				// column names also can be inferred from result of UDTF
				for (int i = 1; i < selExpr.getChildCount(); i++) {
					ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
					switch (selExprChild.getType()) {
						case HiveASTParser.Identifier:
							udtfColAliases.add(unescapeIdentifier(selExprChild.getText().toLowerCase()));
							hiveAnalyzer.unparseTranslator.addIdentifierTranslation(selExprChild);
							break;
						case HiveASTParser.TOK_TABALIAS:
							assert (selExprChild.getChildCount() == 1);
							udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
									.getText());
							qb.addAlias(udtfTableAlias);
							hiveAnalyzer.unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
									.getChild(0));
							break;
						default:
							throw new SemanticException("Find invalid token type " + selExprChild.getType()
									+ " in UDTF.");
					}
				}
				LOG.debug("UDTF table alias is " + udtfTableAlias);
				LOG.debug("UDTF col aliases are " + udtfColAliases);
			}

			// 6. Iterate over all expression (after SELECT)
			ASTNode exprList;
			if (genericUDTF != null) {
				exprList = expr;
			} else {
				exprList = selExprList;
			}
			// For UDTF's, skip the function name to get the expressions
			int startPosn = genericUDTF != null ? posn + 1 : posn;
			for (int i = startPosn; i < exprList.getChildCount(); ++i) {

				// 6.1 child can be EXPR AS ALIAS, or EXPR.
				ASTNode child = (ASTNode) exprList.getChild(i);
				boolean hasAsClause = child.getChildCount() == 2;

				// 6.2 EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
				// This check is not needed and invalid when there is a transform b/c
				// the
				// AST's are slightly different.
				if (genericUDTF == null && child.getChildCount() > 2) {
					throw new SemanticException(generateErrorMessage(
							(ASTNode) child.getChild(2), ErrorMsg.INVALID_AS.getMsg()));
				}

				String tabAlias;
				String colAlias;

				if (genericUDTF != null) {
					tabAlias = null;
					colAlias = hiveAnalyzer.getAutogenColAliasPrfxLbl() + i;
					expr = child;
				} else {
					// 6.3 Get rid of TOK_SELEXPR
					expr = (ASTNode) child.getChild(0);
					String[] colRef = HiveParserUtils.getColAlias(child, hiveAnalyzer.getAutogenColAliasPrfxLbl(),
							inputRR, hiveAnalyzer.autogenColAliasPrfxIncludeFuncName(), i);
					tabAlias = colRef[0];
					colAlias = colRef[1];
					if (hasAsClause) {
						hiveAnalyzer.unparseTranslator.addIdentifierTranslation((ASTNode) child
								.getChild(1));
					}
				}

				Map<ASTNode, RelNode> subQueryToRelNode = new HashMap<>();
				boolean isSubQuery = genSubQueryRelNode(qb, expr, srcRel, false, subQueryToRelNode);
				if (isSubQuery) {
					ExprNodeDesc subQueryExpr = hiveAnalyzer.genExprNodeDesc(expr, relToRowResolver.get(srcRel),
							outerRR, subQueryToRelNode, false);
					exprNodeDescs.add(subQueryExpr);

					ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(pos),
							subQueryExpr.getWritableObjectInspector(), tabAlias, false);
					if (!outRR.putWithCheck(tabAlias, colAlias, null, colInfo)) {
						throw new CalciteSemanticException("Cannot add column to RR: " + tabAlias + "."
								+ colAlias + " => " + colInfo + " due to duplication, see previous warnings",
								CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
					}
				} else {

					// 6.4 Build ExprNode corresponding to columns
					if (expr.getType() == HiveASTParser.TOK_ALLCOLREF) {
						pos = hiveAnalyzer.genColListRegex(".*", expr.getChildCount() == 0 ? null :
										getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(), expr, exprNodeDescs,
								excludedColumns, inputRR, starRR, pos, outRR, qb.getAliases(), false /* don't require uniqueness */);
					} else if (expr.getType() == HiveASTParser.TOK_TABLE_OR_COL
							&& !hasAsClause
							&& !inputRR.getIsExprResolver()
							&& HiveParserUtils.isRegex(unescapeIdentifier(expr.getChild(0).getText()), hiveAnalyzer.getConf())) {
						// In case the expression is a regex COL. This can only happen without AS clause
						// We don't allow this for ExprResolver - the Group By case
						pos = hiveAnalyzer.genColListRegex(unescapeIdentifier(expr.getChild(0).getText()),
								null, expr, exprNodeDescs, excludedColumns, inputRR, starRR, pos, outRR,
								qb.getAliases(), true);
					} else if (expr.getType() == HiveASTParser.DOT
							&& expr.getChild(0).getType() == HiveASTParser.TOK_TABLE_OR_COL
							&& inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()))
							&& !hasAsClause
							&& !inputRR.getIsExprResolver()
							&& HiveParserUtils.isRegex(unescapeIdentifier(expr.getChild(1).getText()), hiveAnalyzer.getConf())) {
						// In case the expression is TABLE.COL (col can be regex). This can only happen without AS clause
						// We don't allow this for ExprResolver - the Group By case
						pos = hiveAnalyzer.genColListRegex(
								unescapeIdentifier(expr.getChild(1).getText()),
								unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()),
								expr, exprNodeDescs, excludedColumns, inputRR, starRR, pos,
								outRR, qb.getAliases(), false /* don't require uniqueness */);
					} else if (HiveASTParseUtils.containsTokenOfType(expr, HiveASTParser.TOK_FUNCTIONDI)
							&& !(srcRel instanceof Aggregate)) {
						// Likely a malformed query eg, select hash(distinct c1) from t1;
						throw new CalciteSemanticException("Distinct without an aggregation.",
								CalciteSemanticException.UnsupportedFeature.Distinct_without_an_aggreggation);
					} else {
						// Case when this is an expression
						HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(inputRR);
						// We allow stateful functions in the SELECT list (but nowhere else)
						typeCheckCtx.setAllowStatefulFunctions(true);
						if (!qbp.getDestToGroupBy().isEmpty()) {
							// Special handling of grouping function
							expr = rewriteGroupingFunctionAST(hiveAnalyzer.getGroupByForClause(qbp, selClauseName), expr,
									!cubeRollupGrpSetPresent);
						}
						ExprNodeDesc exp = hiveAnalyzer.genExprNodeDesc(expr, inputRR, typeCheckCtx);
						String recommended = hiveAnalyzer.recommendName(exp, colAlias);
						if (recommended != null && outRR.get(null, recommended) == null) {
							colAlias = recommended;
						}
						exprNodeDescs.add(exp);

						ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(pos),
								exp.getWritableObjectInspector(), tabAlias, false);
						colInfo.setSkewedCol((exp instanceof ExprNodeColumnDesc) && ((ExprNodeColumnDesc) exp).isSkewedCol());
//						if (!outRR.putWithCheck(tabAlias, colAlias, null, colInfo)) {
//							throw new CalciteSemanticException("Cannot add column to RR: " + tabAlias + "."
//									+ colAlias + " => " + colInfo + " due to duplication, see previous warnings",
//									CalciteSemanticException.UnsupportedFeature.Duplicates_in_RR);
//						}
						// Hive errors out in case of duplication. We allow it and see what happens.
						outRR.put(tabAlias, colAlias, colInfo);

						if (exp instanceof ExprNodeColumnDesc) {
							ExprNodeColumnDesc colExp = (ExprNodeColumnDesc) exp;
							String[] altMapping = inputRR.getAlternateMappings(colExp.getColumn());
							if (altMapping != null) {
								// TODO: this can overwrite the mapping. Should this be allowed?
								outRR.put(altMapping[0], altMapping[1], colInfo);
							}
						}

						pos++;
					}
				}
			}

			// 7. Convert Hive projections to Calcite
			List<RexNode> calciteColLst = new ArrayList<>();

			HiveParserRexNodeConverter rexNodeConverter = new HiveParserRexNodeConverter(cluster, srcRel.getRowType(),
					outerNameToPos, buildHiveColNameToInputPosMap(exprNodeDescs, inputRR), relToRowResolver.get(srcRel),
					outerRR, 0, false, subqueryId);
			for (ExprNodeDesc colExpr : exprNodeDescs) {
				RexNode calciteCol = rexNodeConverter.convert(colExpr);
				calciteCol = convertNullLiteral(calciteCol).accept(funcConverter);
				calciteColLst.add(calciteCol);
			}

			// 8. Build Calcite Rel
			RelNode res;
			if (genericUDTF != null) {
				// The basic idea for CBO support of UDTF is to treat UDTF as a special project.
				res = genUDTFPlan(genericUDTF, genericUDTFName, udtfTableAlias, udtfColAliases, qb, calciteColLst,
						outRR.getColumnInfos(), srcRel, true);
			} else {
				res = genSelectRelNode(calciteColLst, outRR, srcRel);
			}

			// 9. Handle select distinct as GBY if there exist windowing functions
			if (selForWindow != null && selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI) {
				ImmutableBitSet groupSet = ImmutableBitSet.range(res.getRowType().getFieldList().size());
				res = LogicalAggregate.create(res, groupSet, Collections.emptyList(), Collections.emptyList());
				HiveParserRowResolver groupByOutputRowResolver = new HiveParserRowResolver();
				for (int i = 0; i < outRR.getColumnInfos().size(); i++) {
					ColumnInfo colInfo = outRR.getColumnInfos().get(i);
					ColumnInfo newColInfo = new ColumnInfo(colInfo.getInternalName(),
							colInfo.getType(), colInfo.getTabAlias(), colInfo.getIsVirtualCol());
					groupByOutputRowResolver.put(colInfo.getTabAlias(), colInfo.getAlias(), newColInfo);
				}
				relToHiveColNameCalcitePosMap.put(res, buildHiveToCalciteColumnMap(groupByOutputRowResolver));
				relToRowResolver.put(res, groupByOutputRowResolver);
			}

			return res;
		}

		// flink doesn't support type NULL, so we need to convert such literals
		private RexNode convertNullLiteral(RexNode rexNode) {
			if (rexNode instanceof RexLiteral) {
				RexLiteral literal = (RexLiteral) rexNode;
				if (literal.isNull() && literal.getTypeName() == SqlTypeName.NULL) {
					return cluster.getRexBuilder().makeNullLiteral(cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
				}
			}
			return rexNode;
		}

		private RelNode genUDTFPlan(GenericUDTF genericUDTF, String genericUDTFName, String outputTableAlias,
				List<String> colAliases, HiveParserQB qb, List<RexNode> operands, List<ColumnInfo> opColInfos,
				RelNode input, boolean inSelect) throws SemanticException {
			// No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
			HiveParserQBParseInfo qbp = qb.getParseInfo();
			if (inSelect && !qbp.getDestToGroupBy().isEmpty()) {
				throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
			}
			if (inSelect && !qbp.getDestToDistributeBy().isEmpty()) {
				throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
			}
			if (inSelect && !qbp.getDestToSortBy().isEmpty()) {
				throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
			}
			if (inSelect && !qbp.getDestToClusterBy().isEmpty()) {
				throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
			}
			if (inSelect && !qbp.getAliasToLateralViews().isEmpty()) {
				throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
			}

			LOG.debug("Table alias: " + outputTableAlias + " Col aliases: " + colAliases);

			// Create the object inspector for the input columns and initialize the UDTF
			ArrayList<String> colNames = new ArrayList<>();
			ObjectInspector[] colOIs = new ObjectInspector[opColInfos.size()];
			for (int i = 0; i < opColInfos.size(); i++) {
				colNames.add(opColInfos.get(i).getInternalName());
				colOIs[i] = opColInfos.get(i).getObjectInspector();
			}
			StandardStructObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, Arrays.asList(colOIs));
			StructObjectInspector outputOI = genericUDTF.initialize(rowOI);

			if (colAliases.isEmpty()) {
				// user did not specify alias names, infer names from outputOI
				for (StructField field : outputOI.getAllStructFieldRefs()) {
					colAliases.add(field.getFieldName());
				}
			}
			// Make sure that the number of column aliases in the AS clause matches the number of columns output by the UDTF
			int numOutputCols = outputOI.getAllStructFieldRefs().size();
			int numSuppliedAliases = colAliases.size();
			if (numOutputCols != numSuppliedAliases) {
				throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg("expected " + numOutputCols
						+ " aliases " + "but got " + numSuppliedAliases));
			}

			// Generate the output column info's / row resolver using internal names.
			ArrayList<ColumnInfo> udtfOutputCols = new ArrayList<>();

			Iterator<String> colAliasesIter = colAliases.iterator();
			for (StructField sf : outputOI.getAllStructFieldRefs()) {
				String colAlias = colAliasesIter.next();
				assert (colAlias != null);

				// Since the UDTF operator feeds into a LVJ operator that will rename all the internal names,
				// we can just use field name from the UDTF's OI as the internal name
				ColumnInfo col = new ColumnInfo(sf.getFieldName(),
						TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()),
						outputTableAlias, false);
				udtfOutputCols.add(col);
			}

			// Create the row resolver for the table function scan
			HiveParserRowResolver udtfOutRR = new HiveParserRowResolver();
			for (int i = 0; i < udtfOutputCols.size(); i++) {
				udtfOutRR.put(outputTableAlias, colAliases.get(i), udtfOutputCols.get(i));
			}

			// Build row type from field <type, name>
			RelDataType retType = HiveParserTypeConverter.getType(cluster, udtfOutRR, null);

			List<RelDataType> argTypes = new ArrayList<>();

			RelDataTypeFactory dtFactory = cluster.getRexBuilder().getTypeFactory();
			for (ColumnInfo ci : opColInfos) {
				argTypes.add(HiveParserUtils.toRelDataType(ci.getType(), dtFactory));
			}

			SqlOperator calciteOp = HiveParserSqlFunctionConverter.getCalciteOperator(genericUDTFName, genericUDTF, argTypes, retType);

			RexNode rexNode = cluster.getRexBuilder().makeCall(calciteOp, operands);

			// convert the rex call
			ConvertTableFunctionCopier udtfConverter = new ConvertTableFunctionCopier(cluster, input,
					frameworkConfig.getOperatorTable(), catalogReader.nameMatcher());
			RexCall convertedCall = (RexCall) rexNode.accept(udtfConverter);

			SqlOperator convertedOperator = convertedCall.getOperator();
			Preconditions.checkState(convertedOperator instanceof SqlUserDefinedTableFunction,
					"Expect operator to be " + SqlUserDefinedTableFunction.class.getSimpleName() +
							", actually got " + convertedOperator.getClass().getSimpleName());

			// TODO: how to decide this?
			Type elementType = Object[].class;
			// create LogicalTableFunctionScan
			RelNode tableFunctionScan = LogicalTableFunctionScan.create(input.getCluster(), Collections.emptyList(),
					convertedCall, elementType, retType, null);

			RelNode correlRel;
			RexBuilder rexBuilder = cluster.getRexBuilder();
			// find correlation in the converted call
			Pair<List<CorrelationId>, ImmutableBitSet> correlUse = getCorrelationUse(convertedCall);
			// create correlate node
			if (correlUse == null) {
				correlRel = plannerContext.createRelBuilder(catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase())
						.push(input)
						.push(tableFunctionScan)
						.join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
						.build();
			} else {
				if (correlUse.left.size() > 1) {
					tableFunctionScan = DeduplicateCorrelateVariables.go(rexBuilder, correlUse.left.get(0),
							Util.skip(correlUse.left), tableFunctionScan);
				}
				correlRel = LogicalCorrelate.create(input, tableFunctionScan, correlUse.left.get(0), correlUse.right, JoinRelType.INNER);
			}

			// Add new rel & its RR to the maps
			relToHiveColNameCalcitePosMap.put(tableFunctionScan, buildHiveToCalciteColumnMap(udtfOutRR));
			relToRowResolver.put(tableFunctionScan, udtfOutRR);

			HiveParserRowResolver correlRR = HiveParserRowResolver.getCombinedRR(relToRowResolver.get(input), relToRowResolver.get(tableFunctionScan));
			relToHiveColNameCalcitePosMap.put(correlRel, buildHiveToCalciteColumnMap(correlRR));
			relToRowResolver.put(correlRel, correlRR);

			if (!inSelect) {
				return correlRel;
			}

			// create project node
			List<RexNode> projects = new ArrayList<>();
			HiveParserRowResolver projectRR = new HiveParserRowResolver();
			int j = 0;
			for (int i = input.getRowType().getFieldCount(); i < correlRel.getRowType().getFieldCount(); i++) {
				projects.add(cluster.getRexBuilder().makeInputRef(correlRel, i));
				ColumnInfo inputColInfo = correlRR.getRowSchema().getSignature().get(i);
				String colAlias = inputColInfo.getAlias();
				ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(j++),
						inputColInfo.getObjectInspector(), null, false);
				projectRR.put(null, colAlias, colInfo);
			}
			RelNode projectNode = LogicalProject.create(correlRel, Collections.emptyList(), projects, tableFunctionScan.getRowType());
			relToHiveColNameCalcitePosMap.put(projectNode, buildHiveToCalciteColumnMap(projectRR));
			relToRowResolver.put(projectNode, projectRR);
			return projectNode;
		}

		private RelNode genLogicalPlan(HiveParserQBExpr qbexpr) throws SemanticException {
			switch (qbexpr.getOpcode()) {
				case NULLOP:
					return genLogicalPlan(qbexpr.getQB(), false, null, null);
				case UNION:
				case INTERSECT:
				case INTERSECTALL:
				case EXCEPT:
				case EXCEPTALL:
					RelNode qbexpr1Ops = genLogicalPlan(qbexpr.getQBExpr1());
					RelNode qbexpr2Ops = genLogicalPlan(qbexpr.getQBExpr2());
					return genSetOpLogicalPlan(qbexpr.getOpcode(), qbexpr.getAlias(), qbexpr.getQBExpr1()
							.getAlias(), qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
				default:
					return null;
			}
		}

		private RelNode genLogicalPlan(HiveParserQB qb, boolean outerMostQB, Map<String, Integer> outerNameToPosMap,
				HiveParserRowResolver outerRR) throws SemanticException {
			RelNode res;

			// First generate all the opInfos for the elements in the from clause
			Map<String, RelNode> aliasToRel = new HashMap<>();

			// 0. Check if we can handle the SubQuery;
			// canHandleQbForCbo returns null if the query can be handled.
			String reason = HiveParserUtils.canHandleQbForCbo(hiveAnalyzer.getQueryProperties());
			if (reason != null) {
				String msg = "CBO can not handle Sub Query";
				if (LOG.isDebugEnabled()) {
					LOG.debug(msg + " because it: " + reason);
				}
				throw new CalciteSemanticException(msg, CalciteSemanticException.UnsupportedFeature.Subquery);
			}

			// 1. Build Rel For Src (SubQuery, TS, Join)
			// 1.1. Recurse over the subqueries to fill the subquery part of the plan
			for (String subqAlias : qb.getSubqAliases()) {
				HiveParserQBExpr qbexpr = qb.getSubqForAlias(subqAlias);
				RelNode relNode = genLogicalPlan(qbexpr);
				aliasToRel.put(subqAlias, relNode);
				if (qb.getViewToTabSchema().containsKey(subqAlias)) {
					if (relNode instanceof Project) {
						if (viewProjectToTableSchema == null) {
							viewProjectToTableSchema = new LinkedHashMap<>();
						}
						viewProjectToTableSchema.put((Project) relNode, qb.getViewToTabSchema().get(subqAlias));
					} else {
						throw new SemanticException("View " + subqAlias + " is corresponding to "
								+ relNode.toString() + ", rather than a Project.");
					}
				}
			}

			// 1.2 Recurse over all the source tables
			for (String tableAlias : qb.getTabAliases()) {
				RelNode op = genTableLogicalPlan(tableAlias, qb);
				aliasToRel.put(tableAlias, op);
			}

			if (aliasToRel.isEmpty()) {
				// // This may happen for queries like select 1; (no source table)
				// We can do following which is same, as what Hive does.
				// With this, we will be able to generate Calcite plan.
				// qb.getMetaData().setSrcForAlias(DUMMY_TABLE, getDummyTable());
				// RelNode op = genTableLogicalPlan(DUMMY_TABLE, qb);
				// qb.addAlias(DUMMY_TABLE);
				// qb.setTabAlias(DUMMY_TABLE, DUMMY_TABLE);
				// aliasToRel.put(DUMMY_TABLE, op);
				// However, Hive trips later while trying to get Metadata for this dummy
				// table
				// So, for now lets just disable this. Anyway there is nothing much to
				// optimize in such cases.
//				throw new CalciteSemanticException("Unsupported", CalciteSemanticException.UnsupportedFeature.Others);
				RelNode dummySrc = LogicalValues.createOneRow(cluster);
				aliasToRel.put(DUMMY_TABLE, dummySrc);
				HiveParserRowResolver dummyRR = new HiveParserRowResolver();
				dummyRR.put(DUMMY_TABLE, "dummy_col",
						new ColumnInfo(getColumnInternalName(0), TypeInfoFactory.intTypeInfo, DUMMY_TABLE, false));
				relToRowResolver.put(dummySrc, dummyRR);
				relToHiveColNameCalcitePosMap.put(dummySrc, buildHiveToCalciteColumnMap(dummyRR));
			}

			if (!qb.getParseInfo().getAliasToLateralViews().isEmpty()) {
				// process lateral views
				res = genLateralViewPlan(qb, aliasToRel);
			} else if (qb.getParseInfo().getJoinExpr() != null) {
				// 1.3 process join
				res = genJoinLogicalPlan(qb.getParseInfo().getJoinExpr(), aliasToRel);
			} else {
				// If no join then there should only be either 1 TS or 1 SubQuery
				res = aliasToRel.values().iterator().next();
			}

			// 2. Build Rel for where Clause
			RelNode filterRel = genFilterLogicalPlan(qb, res, outerNameToPosMap, outerRR, false);
			res = (filterRel == null) ? res : filterRel;
			RelNode starSrcRel = res;

			// 3. Build Rel for GB Clause
			RelNode gbRel = genGBLogicalPlan(qb, res);
			res = gbRel == null ? res : gbRel;

			// 4. Build Rel for GB Having Clause
			RelNode gbHavingRel = genGBHavingLogicalPlan(qb, res);
			res = gbHavingRel == null ? res : gbHavingRel;

			// 5. Build Rel for Select Clause
			RelNode selectRel = genSelectLogicalPlan(qb, res, starSrcRel, outerNameToPosMap, outerRR);
			res = selectRel == null ? res : selectRel;

			// 6. Build Rel for OB Clause
			Pair<Sort, RelNode> obAndTopProj = genOBLogicalPlan(qb, res, outerMostQB);
			Sort orderRel = obAndTopProj.getKey();
			RelNode topConstrainingProjRel = obAndTopProj.getValue();
			res = orderRel == null ? res : orderRel;

			// Build Rel for SortBy/ClusterBy/DistributeBy. It can happen only if we don't have OrderBy.
			if (orderRel == null) {
				Pair<RelNode, RelNode> distAndTopProj = genDistSortBy(qb, res, outerMostQB);
				RelNode distRel = distAndTopProj.getKey();
				topConstrainingProjRel = distAndTopProj.getValue();
				res = distRel == null ? res : distRel;
			}

			// 7. Build Rel for Limit Clause
			Sort limitRel = genLimitLogicalPlan(qb, res);
			if (limitRel != null) {
				if (orderRel != null) {
					// merge limit into the order-by node
					HiveParserRowResolver orderRR = relToRowResolver.remove(orderRel);
					Map<String, Integer> orderColNameToPos = relToHiveColNameCalcitePosMap.remove(orderRel);
					res = LogicalSort.create(orderRel.getInput(), orderRel.collation, limitRel.offset, limitRel.fetch);
					relToRowResolver.put(res, orderRR);
					relToHiveColNameCalcitePosMap.put(res, orderColNameToPos);

					relToRowResolver.remove(limitRel);
					relToHiveColNameCalcitePosMap.remove(limitRel);
				} else {
					res = limitRel;
				}
			}

			// 8. Introduce top constraining select if needed.
			// NOTES:
			// 1. Calcite can not take an expr in OB; hence it needs to be added as VC
			// in the input select; In such cases we need to introduce a select on top
			// to ensure VC is not visible beyond Limit, OB.
			// 2. Hive can not preserve order across select. In subqueries OB is used
			// to get a deterministic set of tuples from following limit. Hence we
			// introduce the constraining select above Limit (if present) instead of  OB.
			// 3. The top level OB will not introduce constraining select due to Hive
			// limitation(#2) stated above. The RR for OB will not include VC. Thus
			// Result Schema will not include exprs used by top OB. During AST Conv,
			// in the PlanModifierForASTConv we would modify the top level OB to
			// migrate exprs from input sel to SortRel (Note that Calcite doesn't
			// support this; but since we are done with Calcite at this point its OK).
			//
			// Hive-compatibility: we introduce the constraining SEL even for top-level OB. Same as what Flink generates.
			if (topConstrainingProjRel != null) {
				List<RexNode> originalInputRefs = topConstrainingProjRel.getRowType()
						.getFieldList().stream().map(input -> new RexInputRef(input.getIndex(), input.getType()))
						.collect(Collectors.toList());
				HiveParserRowResolver topConstrainingProjRR = new HiveParserRowResolver();
				if (!HiveParserRowResolver.add(topConstrainingProjRR, relToRowResolver.get(topConstrainingProjRel))) {
					LOG.warn("Duplicates detected when adding columns to RR: see previous message");
				}
				res = genSelectRelNode(originalInputRefs, topConstrainingProjRR, res);
			}

			// 9. In case this HiveParserQB corresponds to subquery then modify its RR to point
			// to subquery alias
			// TODO: cleanup this
			if (qb.getParseInfo().getAlias() != null) {
				HiveParserRowResolver rr = relToRowResolver.get(res);
				HiveParserRowResolver newRR = new HiveParserRowResolver();
				String alias = qb.getParseInfo().getAlias();
				for (ColumnInfo colInfo : rr.getColumnInfos()) {
					String name = colInfo.getInternalName();
					String[] tmp = rr.reverseLookup(name);
					if ("".equals(tmp[0]) || tmp[1] == null) {
						// ast expression is not a valid column name for table
						tmp[1] = colInfo.getInternalName();
					}
					ColumnInfo newColInfo = new ColumnInfo(colInfo);
					newColInfo.setTabAlias(alias);
					newRR.put(alias, tmp[1], newColInfo);
				}
				relToRowResolver.put(res, newRR);
				relToHiveColNameCalcitePosMap.put(res, buildHiveToCalciteColumnMap(newRR));
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Created Plan for Query Block " + qb.getId());
			}

			hiveAnalyzer.setQB(qb);
			return res;
		}

		private RelNode genLateralViewPlan(HiveParserQB qb, Map<String, RelNode> aliasToRel)
				throws SemanticException {
			Map<String, ArrayList<ASTNode>> aliasToLateralViews = qb.getParseInfo().getAliasToLateralViews();
			Preconditions.checkArgument(aliasToLateralViews.size() == 1, "We only support lateral views for 1 alias");
			Map.Entry<String, ArrayList<ASTNode>> entry = aliasToLateralViews.entrySet().iterator().next();
			String alias = entry.getKey();
			RelNode res = null;
			List<ASTNode> lateralViews = entry.getValue();
			for (ASTNode lateralView : lateralViews) {
				Preconditions.checkArgument(lateralView.getChildCount() == 2);
				// this is the 1st lateral view
				if (res == null) {
					// LHS can be table or sub-query
					res = aliasToRel.get(alias);
				}
				Preconditions.checkState(res != null, "Failed to decide LHS table for current lateral view");
				HiveParserRowResolver inputRR = relToRowResolver.get(res);
				HiveParserUtils.LateralViewInfo info = HiveParserUtils.extractLateralViewInfo(lateralView, inputRR, hiveAnalyzer);
				HiveParserRexNodeConverter rexNodeConverter = new HiveParserRexNodeConverter(cluster, res.getRowType(),
						relToHiveColNameCalcitePosMap.get(res), 0, false);
				List<RexNode> operands = new ArrayList<>(info.getOperands().size());
				for (ExprNodeDesc exprDesc : info.getOperands()) {
					operands.add(rexNodeConverter.convert(exprDesc).accept(funcConverter));
				}
				res = genUDTFPlan(info.getFunc(), info.getFuncName(), info.getTabAlias(), info.getColAliases(), qb,
						operands, info.getOperandColInfos(), res, false);
			}
			return res;
		}

		private RelNode genGBHavingLogicalPlan(HiveParserQB qb, RelNode srcRel)
				throws SemanticException {
			RelNode gbFilter = null;
			HiveParserQBParseInfo qbp = getQBParseInfo(qb);
			String destClauseName = qbp.getClauseNames().iterator().next();
			ASTNode havingClause = qbp.getHavingForClause(qbp.getClauseNames().iterator().next());

			if (havingClause != null) {
				if (!(srcRel instanceof Aggregate)) {
					// ill-formed query like select * from t1 having c1 > 0;
					throw new CalciteSemanticException("Having clause without any group-by.",
							CalciteSemanticException.UnsupportedFeature.Having_clause_without_any_groupby);
				}
				ASTNode targetNode = (ASTNode) havingClause.getChild(0);
				validateNoHavingReferenceToAlias(qb, targetNode, relToRowResolver.get(srcRel));
				if (!qbp.getDestToGroupBy().isEmpty()) {
					final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
							|| !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());
					// Special handling of grouping function
					targetNode = rewriteGroupingFunctionAST(hiveAnalyzer.getGroupByForClause(qbp, destClauseName), targetNode,
							!cubeRollupGrpSetPresent);
				}
				gbFilter = genFilterRelNode(qb, targetNode, srcRel, null, null, true);
			}

			return gbFilter;
		}

		// We support having referring alias just as in hive's semantic analyzer. This check only prints a warning now.
		private void validateNoHavingReferenceToAlias(HiveParserQB qb, ASTNode havingExpr, HiveParserRowResolver inputRR)
				throws SemanticException {
			HiveParserQBParseInfo qbPI = qb.getParseInfo();
			Map<ASTNode, String> exprToAlias = qbPI.getAllExprToColumnAlias();

			for (Map.Entry<ASTNode, String> exprAndAlias : exprToAlias.entrySet()) {
				final ASTNode expr = exprAndAlias.getKey();
				final String alias = exprAndAlias.getValue();
				// put the alias in input RR so that we can generate ExprNodeDesc with it
				if (inputRR.getExpression(expr) != null) {
					inputRR.put("", alias, inputRR.getExpression(expr));
				}
				final Set<Object> aliasReferences = new HashSet<>();

				TreeVisitorAction action = new TreeVisitorAction() {
					@Override
					public Object pre(Object t) {
						if (HiveASTParseDriver.ADAPTOR.getType(t) == HiveASTParser.TOK_TABLE_OR_COL) {
							Object c = HiveASTParseDriver.ADAPTOR.getChild(t, 0);
							if (c != null && HiveASTParseDriver.ADAPTOR.getType(c) == HiveASTParser.Identifier
									&& HiveASTParseDriver.ADAPTOR.getText(c).equals(alias)) {
								aliasReferences.add(t);
							}
						}
						return t;
					}

					@Override
					public Object post(Object t) {
						return t;
					}
				};
				new TreeVisitor(HiveASTParseDriver.ADAPTOR).visit(havingExpr, action);

				if (aliasReferences.size() > 0) {
					String havingClause = hiveAnalyzer.ctx.getTokenRewriteStream().toString(
							havingExpr.getTokenStartIndex(), havingExpr.getTokenStopIndex());
					String msg = String.format("Encountered Select alias '%s' in having clause '%s'"
							+ " This is non standard behavior.", alias, havingClause);
					LOG.warn(msg);
				}
			}

		}

		private Map<String, Integer> buildHiveToCalciteColumnMap(HiveParserRowResolver rr) {
			Map<String, Integer> map = new HashMap<>();
			for (ColumnInfo ci : rr.getRowSchema().getSignature()) {
				map.put(ci.getInternalName(), rr.getPosition(ci.getInternalName()));
			}
			return Collections.unmodifiableMap(map);
		}

		private com.google.common.collect.ImmutableMap<String, Integer> buildHiveColNameToInputPosMap(
				List<ExprNodeDesc> colList, HiveParserRowResolver inputRR) {
			// Build a map of Hive column Names (ExprNodeColumnDesc Name)
			// to the positions of those projections in the input
			Map<Integer, ExprNodeDesc> hashCodeTocolumnDescMap = new HashMap<Integer, ExprNodeDesc>();
			ExprNodeDescUtils.getExprNodeColumnDesc(colList, hashCodeTocolumnDescMap);
			com.google.common.collect.ImmutableMap.Builder<String, Integer> hiveColNameToInputPosMapBuilder =
					new com.google.common.collect.ImmutableMap.Builder<String, Integer>();
			String exprNodecolName;
			for (ExprNodeDesc exprDesc : hashCodeTocolumnDescMap.values()) {
				exprNodecolName = ((ExprNodeColumnDesc) exprDesc).getColumn();
				hiveColNameToInputPosMapBuilder.put(exprNodecolName, inputRR.getPosition(exprNodecolName));
			}

			return hiveColNameToInputPosMapBuilder.build();
		}

		private HiveParserQBParseInfo getQBParseInfo(HiveParserQB qb) {
			return qb.getParseInfo();
		}
	}

	private enum TableType {
		DRUID,
		NATIVE
	}

	private boolean canCBOHandleAst(ASTNode ast, HiveParserQB qb, HiveParserPreCboCtx cboCtx) {
		QueryProperties queryProperties = hiveAnalyzer.getQueryProperties();
		int root = ast.getToken().getType();
		boolean isSupportedRoot = root == HiveASTParser.TOK_QUERY || root == HiveASTParser.TOK_EXPLAIN
				|| qb.isCTAS() || qb.isMaterializedView();
		// To support queries without a source table
		// If it's neither a query nor a multi-insert, consider it as an ordinary insert. Implement our own PreCboCtx to be sure.
		boolean isSupportedType = qb.getIsQuery() || qb.isCTAS() || qb.isMaterializedView() || !queryProperties.hasMultiDestQuery();
		boolean noBadTokens = HiveCalciteUtil.validateASTForUnsupportedTokens(ast);
		boolean result = isSupportedRoot && isSupportedType && noBadTokens;

		if (!result) {
			return false;
		}
		// Now check HiveParserQB in more detail.
		String reason = HiveParserUtils.canHandleQbForCbo(queryProperties);
		if (reason != null) {
			LOG.warn("HiveParser doesn't support the SQL statement because it " + reason);
		}
		return reason == null;
	}

	public List<String> getDestSchemaForClause(String clause) {
		return getQB().getParseInfo().getDestSchemaForClause(clause);
	}

	private static Pair<List<CorrelationId>, ImmutableBitSet> getCorrelationUse(RexCall call) {
		List<CorrelationId> correlIDs = new ArrayList<>();
		ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
		for (RexNode operand : call.getOperands()) {
			if (operand instanceof RexFieldAccess) {
				RexNode expr = ((RexFieldAccess) operand).getReferenceExpr();
				if (expr instanceof RexCorrelVariable) {
					RexCorrelVariable correlVariable = (RexCorrelVariable) expr;
					correlIDs.add(correlVariable.id);
					requiredColumns.set(((RexFieldAccess) operand).getField().getIndex());
				}
			}
		}
		if (correlIDs.isEmpty()) {
			return null;
		}
		return Pair.of(correlIDs, requiredColumns.build());
	}
}
