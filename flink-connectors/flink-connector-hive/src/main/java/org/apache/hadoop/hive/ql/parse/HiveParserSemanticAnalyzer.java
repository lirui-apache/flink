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

import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;

import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.HiveParserContext;
import org.apache.hadoop.hive.ql.HiveParserQueryState;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.HiveParserDefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.TableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.PartitionedTableFunctionSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserPTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.HiveParserWindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.Phase1Ctx;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getQualifiedTableName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getUnescapedName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.getUnescapedUnqualifiedTableName;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.readProps;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.stripQuotes;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeSQLString;
import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.validatePartSpec;

/**
 * Copied from hive-2.3.4 and adapted to our needs.
 */
public class HiveParserSemanticAnalyzer {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParserSemanticAnalyzer.class);

	public static final String DUMMY_TABLE = "_dummy_table";
	public static final String SUBQUERY_TAG_1 = "-subquery1";
	public static final String SUBQUERY_TAG_2 = "-subquery2";

	// Max characters when auto generating the column name with func name
	public static final int AUTOGEN_COLALIAS_PRFX_MAXLENGTH = 20;

	public static final String VALUES_TMP_TABLE_NAME_PREFIX = "Values__Tmp__Table__";

	private HiveParserQB qb;
	private ASTNode ast;
	// a map for the split sampling, from alias to an instance of SplitSample that describes percentage and number.
	private final HashMap<String, SplitSample> nameToSplitSample;
	Map<String, PrunedPartitionList> prunedPartitions;
	protected List<FieldSchema> resultSchema;
	protected CreateViewDesc createVwDesc;
	protected ArrayList<String> viewsExpanded;
	protected ASTNode viewSelect;
	protected final UnparseTranslator unparseTranslator;
	private final GlobalLimitCtx globalLimitCtx;

	// prefix for column names auto generated by hive
	private final String autogenColAliasPrfxLbl;
	private final boolean autogenColAliasPrfxIncludeFuncName;

	// Keep track of view alias to read entity corresponding to the view
	// For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
	// keeps track of aliases for V3, V3:V2, V3:V2:V1.
	// This is used when T is added as an input for the query, the parents of T is
	// derived from the alias V3:V2:V1:T
	private final Map<String, ReadEntity> viewAliasToInput;

	// need merge isDirect flag to input even if the newInput does not have a parent
	private boolean mergeIsDirect;

	// flag for no scan during analyze ... compute statistics
	protected boolean noscan;

	// flag for partial scan during analyze ... compute statistics
	protected boolean partialscan;

	protected volatile boolean disableJoinMerge = false;
	protected final boolean defaultJoinMerge;

	// Capture the CTE definitions in a Query.
	final Map<String, CTEClause> aliasToCTEs;

	// Used to check recursive CTE invocations. Similar to viewsExpanded
	ArrayList<String> ctesExpanded;

	protected HiveParserBaseSemanticAnalyzer.AnalyzeRewriteContext analyzeRewrite;

	// A mapping from a tableName to a table object in metastore.
	Map<String, Table> tabNameToTabObject;

	ColumnAccessInfo columnAccessInfo;

	private final HiveConf conf;

	HiveParserContext ctx;

	QueryProperties queryProperties;

	private final HiveShim hiveShim;

	private final Hive db;

	// ReadEntities that are passed to the hooks.
	protected HashSet<ReadEntity> inputs = new LinkedHashSet<>();

	private final HiveParserQueryState queryState;

	public HiveParserSemanticAnalyzer(HiveParserQueryState queryState, HiveShim hiveShim) throws SemanticException {
		this.queryState = queryState;
		this.conf = queryState.getConf();
		this.hiveShim = hiveShim;
		this.db = BaseSemanticAnalyzer.createHiveDB(conf);
		nameToSplitSample = new HashMap<>();
		prunedPartitions = new HashMap<>();
		tabNameToTabObject = new HashMap<>();
		unparseTranslator = new UnparseTranslator(conf);
		autogenColAliasPrfxLbl = HiveConf.getVar(conf,
				HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL);
		autogenColAliasPrfxIncludeFuncName = HiveConf.getBoolVar(conf,
				HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME);
		queryProperties = new QueryProperties();
		aliasToCTEs = new HashMap<>();
		globalLimitCtx = new GlobalLimitCtx();
		viewAliasToInput = new HashMap<>();
		mergeIsDirect = true;
		noscan = partialscan = false;
		tabNameToTabObject = new HashMap<>();
		defaultJoinMerge = !Boolean.parseBoolean(conf.get("hive.merge.nway.joins", "true"));
		disableJoinMerge = defaultJoinMerge;
	}

	public HiveConf getConf() {
		return conf;
	}

	void initCtx(HiveParserContext context) {
		this.ctx = context;
	}

	QueryProperties getQueryProperties() {
		return queryProperties;
	}

	protected void reset(boolean clearPartsCache) {
		if (clearPartsCache) {
			prunedPartitions.clear();
			//When init(true) combine with genResolvedParseTree, it will generate Resolved Parse tree from syntax tree
			//ReadEntity created under these conditions should be all relevant to the syntax tree even the ones without parents
			//set mergeIsDirect to true here.
			mergeIsDirect = true;
		} else {
			mergeIsDirect = false;
		}
		tabNameToTabObject.clear();
		qb = null;
		ast = null;
		disableJoinMerge = defaultJoinMerge;
		aliasToCTEs.clear();
		nameToSplitSample.clear();
		resultSchema = null;
		createVwDesc = null;
		viewsExpanded = null;
		viewSelect = null;
		ctesExpanded = null;
		globalLimitCtx.disableOpt();
		viewAliasToInput.clear();
		unparseTranslator.clear();
		queryProperties.clear();
	}

	public void doPhase1QBExpr(ASTNode ast, HiveParserQBExpr qbexpr, String id, String alias)
			throws SemanticException {
		doPhase1QBExpr(ast, qbexpr, id, alias, false);
	}

	@SuppressWarnings("nls")
	public void doPhase1QBExpr(ASTNode ast, HiveParserQBExpr qbexpr, String id, String alias, boolean insideView)
			throws SemanticException {

		assert (ast.getToken() != null);
		if (ast.getToken().getType() == HiveASTParser.TOK_QUERY) {
			HiveParserQB qb = new HiveParserQB(id, alias, true);
			qb.setInsideView(insideView);
			Phase1Ctx ctx1 = initPhase1Ctx();
			doPhase1(ast, qb, ctx1, null);

			qbexpr.setOpcode(HiveParserQBExpr.Opcode.NULLOP);
			qbexpr.setQB(qb);
		}
		// setop
		else {
			int type = ast.getToken().getType();
			switch (type) {
				case HiveASTParser.TOK_UNIONALL:
					qbexpr.setOpcode(HiveParserQBExpr.Opcode.UNION);
					break;
				case HiveASTParser.TOK_INTERSECTALL:
					qbexpr.setOpcode(HiveParserQBExpr.Opcode.INTERSECTALL);
					break;
				case HiveASTParser.TOK_INTERSECTDISTINCT:
					qbexpr.setOpcode(HiveParserQBExpr.Opcode.INTERSECT);
					break;
				case HiveASTParser.TOK_EXCEPTALL:
					qbexpr.setOpcode(HiveParserQBExpr.Opcode.EXCEPTALL);
					break;
				case HiveASTParser.TOK_EXCEPTDISTINCT:
					qbexpr.setOpcode(HiveParserQBExpr.Opcode.EXCEPT);
					break;
				default:
					throw new SemanticException("Unsupported set operator type: " + type);
			}
			// query 1
			assert (ast.getChild(0) != null);
			HiveParserQBExpr qbexpr1 = new HiveParserQBExpr(alias + SUBQUERY_TAG_1);
			doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id + SUBQUERY_TAG_1, alias
					+ SUBQUERY_TAG_1, insideView);
			qbexpr.setQBExpr1(qbexpr1);

			// query 2
			assert (ast.getChild(1) != null);
			HiveParserQBExpr qbexpr2 = new HiveParserQBExpr(alias + SUBQUERY_TAG_2);
			doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id + SUBQUERY_TAG_2, alias
					+ SUBQUERY_TAG_2, insideView);
			qbexpr.setQBExpr2(qbexpr2);
		}
	}

	private LinkedHashMap<String, ASTNode> doPhase1GetAggregationsFromSelect(
			ASTNode selExpr, HiveParserQB qb, String dest) throws SemanticException {

		// Iterate over the selects search for aggregation Trees.
		// Use String as keys to eliminate duplicate trees.
		LinkedHashMap<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
		List<ASTNode> wdwFns = new ArrayList<ASTNode>();
		for (int i = 0; i < selExpr.getChildCount(); ++i) {
			ASTNode function = (ASTNode) selExpr.getChild(i);
			if (function.getType() == HiveASTParser.TOK_SELEXPR ||
					function.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
				function = (ASTNode) function.getChild(0);
			}
			doPhase1GetAllAggregations(function, aggregationTrees, wdwFns);
		}

		// window based aggregations are handled differently
		for (ASTNode wdwFn : wdwFns) {
			HiveParserWindowingSpec spec = qb.getWindowingSpec(dest);
			if (spec == null) {
				queryProperties.setHasWindowing(true);
				spec = new HiveParserWindowingSpec();
				qb.addDestToWindowingSpec(dest, spec);
			}
			HashMap<String, ASTNode> wExprsInDest = qb.getParseInfo().getWindowingExprsForClause(dest);
			int wColIdx = spec.getWindowExpressions() == null ? 0 : spec.getWindowExpressions().size();
			WindowFunctionSpec wFnSpec = processWindowFunction(wdwFn,
					(ASTNode) wdwFn.getChild(wdwFn.getChildCount() - 1));
			// If this is a duplicate invocation of a function; don't add to HiveParserWindowingSpec.
			if (wExprsInDest != null &&
					wExprsInDest.containsKey(wFnSpec.getExpression().toStringTree())) {
				continue;
			}
			wFnSpec.setAlias(wFnSpec.getName() + "_window_" + wColIdx);
			spec.addWindowFunction(wFnSpec);
			qb.getParseInfo().addWindowingExprToClause(dest, wFnSpec.getExpression());
		}

		return aggregationTrees;
	}

	private void doPhase1GetColumnAliasesFromSelect(
			ASTNode selectExpr, HiveParserQBParseInfo qbp) {
		for (int i = 0; i < selectExpr.getChildCount(); ++i) {
			ASTNode selExpr = (ASTNode) selectExpr.getChild(i);
			if ((selExpr.getToken().getType() == HiveASTParser.TOK_SELEXPR)
					&& (selExpr.getChildCount() == 2)) {
				String columnAlias = unescapeIdentifier(selExpr.getChild(1).getText());
				qbp.setExprToColumnAlias((ASTNode) selExpr.getChild(0), columnAlias);
			}
		}
	}

	// DFS-scan the expressionTree to find all aggregation subtrees and put them in aggregations.
	private void doPhase1GetAllAggregations(ASTNode expressionTree,
			HashMap<String, ASTNode> aggregations, List<ASTNode> wdwFns) throws SemanticException {
		int exprTokenType = expressionTree.getToken().getType();
		if (exprTokenType == HiveASTParser.TOK_SUBQUERY_EXPR) {
			//since now we have scalar subqueries we can get subquery expression in having
			// we don't want to include aggregate from within subquery
			return;
		}

		if (exprTokenType == HiveASTParser.TOK_FUNCTION
				|| exprTokenType == HiveASTParser.TOK_FUNCTIONDI
				|| exprTokenType == HiveASTParser.TOK_FUNCTIONSTAR) {
			assert (expressionTree.getChildCount() != 0);
			if (expressionTree.getChild(expressionTree.getChildCount() - 1).getType()
					== HiveASTParser.TOK_WINDOWSPEC) {
				// If it is a windowing spec, we include it in the list
				// Further, we will examine its children AST nodes to check whether
				// there are aggregation functions within
				wdwFns.add(expressionTree);
				doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(expressionTree.getChildCount() - 1),
						aggregations, wdwFns);
				return;
			}
			if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
				String functionName = unescapeIdentifier(expressionTree.getChild(0)
						.getText());
				// Validate the function name
				if (HiveParserUtils.getFunctionInfo(functionName) == null) {
					throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
				}
				if (FunctionRegistry.impliesOrder(functionName)) {
					throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
				}
				if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
					if (containsLeadLagUDF(expressionTree)) {
						throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
					}
					aggregations.put(expressionTree.toStringTree(), expressionTree);
					FunctionInfo fi = HiveParserUtils.getFunctionInfo(functionName);
					if (!fi.isNative()) {
						unparseTranslator.addIdentifierTranslation((ASTNode) expressionTree
								.getChild(0));
					}
					return;
				}
			}
		}
		for (int i = 0; i < expressionTree.getChildCount(); i++) {
			doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(i),
					aggregations, wdwFns);
		}
	}

	private List<ASTNode> doPhase1GetDistinctFuncExprs(
			HashMap<String, ASTNode> aggregationTrees) throws SemanticException {
		List<ASTNode> exprs = new ArrayList<ASTNode>();
		for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
			ASTNode value = entry.getValue();
			assert (value != null);
			if (value.getToken().getType() == HiveASTParser.TOK_FUNCTIONDI) {
				exprs.add(value);
			}
		}
		return exprs;
	}

	int[] findTabRefIdxs(ASTNode tabref) {
		assert tabref.getType() == HiveASTParser.TOK_TABREF;
		int aliasIndex = 0;
		int propsIndex = -1;
		int tsampleIndex = -1;
		int ssampleIndex = -1;
		for (int index = 1; index < tabref.getChildCount(); index++) {
			ASTNode ct = (ASTNode) tabref.getChild(index);
			if (ct.getToken().getType() == HiveASTParser.TOK_TABLEBUCKETSAMPLE) {
				tsampleIndex = index;
			} else if (ct.getToken().getType() == HiveASTParser.TOK_TABLESPLITSAMPLE) {
				ssampleIndex = index;
			} else if (ct.getToken().getType() == HiveASTParser.TOK_TABLEPROPERTIES) {
				propsIndex = index;
			} else {
				aliasIndex = index;
			}
		}
		return new int[]{aliasIndex, propsIndex, tsampleIndex, ssampleIndex};
	}

	String findSimpleTableName(ASTNode tabref, int aliasIndex) {
		assert tabref.getType() == HiveASTParser.TOK_TABREF;
		ASTNode tableTree = (ASTNode) (tabref.getChild(0));

		String alias;
		if (aliasIndex != 0) {
			alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
		} else {
			alias = getUnescapedUnqualifiedTableName(tableTree);
		}
		return alias;
	}

	/**
	 * Goes though the tabref tree and finds the alias for the table. Once found,
	 * it records the table name-> alias association in aliasToTabs. It also makes
	 * an association from the alias to the table AST in parse info.
	 *
	 * @return the alias of the table
	 */
	private String processTable(HiveParserQB qb, ASTNode tabref) throws SemanticException {
		// For each table reference get the table name
		// and the alias (if alias is not present, the table name
		// is used as an alias)
		int[] indexes = findTabRefIdxs(tabref);
		int aliasIndex = indexes[0];
		int propsIndex = indexes[1];
		int tsampleIndex = indexes[2];
		int ssampleIndex = indexes[3];

		ASTNode tableTree = (ASTNode) (tabref.getChild(0));

		String tabIdName = getUnescapedName(tableTree).toLowerCase();

		String alias = findSimpleTableName(tabref, aliasIndex);

		if (propsIndex >= 0) {
			Tree propsAST = tabref.getChild(propsIndex);
			Map<String, String> props = DDLSemanticAnalyzer.getProps((ASTNode) propsAST.getChild(0));
			// We get the information from Calcite.
			if ("TRUE".equals(props.get("insideView"))) {
				qb.getAliasInsideView().add(alias.toLowerCase());
			}
			qb.setTabProps(alias, props);
		}

		// If the alias is already there then we have a conflict
		if (qb.exists(alias)) {
			throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref
					.getChild(aliasIndex)));
		}
		if (tsampleIndex >= 0) {
			ASTNode sampleClause = (ASTNode) tabref.getChild(tsampleIndex);
			ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
			if (sampleClause.getChildCount() > 2) {
				for (int i = 2; i < sampleClause.getChildCount(); i++) {
					sampleCols.add((ASTNode) sampleClause.getChild(i));
				}
			}
			// TODO: For now only support sampling on up to two columns
			// Need to change it to list of columns
			if (sampleCols.size() > 2) {
				throw new SemanticException(HiveParserUtils.generateErrorMessage(
						(ASTNode) tabref.getChild(0),
						ErrorMsg.SAMPLE_RESTRICTION.getMsg()));
			}
			TableSample tabSample = new TableSample(
					unescapeIdentifier(sampleClause.getChild(0).getText()),
					unescapeIdentifier(sampleClause.getChild(1).getText()),
					sampleCols);
			qb.getParseInfo().setTabSample(alias, tabSample);
			if (unparseTranslator.isEnabled()) {
				for (ASTNode sampleCol : sampleCols) {
					unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
							.getChild(0));
				}
			}
		} else if (ssampleIndex >= 0) {
			ASTNode sampleClause = (ASTNode) tabref.getChild(ssampleIndex);

			Tree type = sampleClause.getChild(0);
			Tree numerator = sampleClause.getChild(1);
			String value = unescapeIdentifier(numerator.getText());

			SplitSample sample;
			if (type.getType() == HiveASTParser.TOK_PERCENT) {
				assertCombineInputFormat(numerator, "Percentage");
				Double percent = Double.valueOf(value).doubleValue();
				if (percent < 0 || percent > 100) {
					throw new SemanticException(HiveParserUtils.generateErrorMessage((ASTNode) numerator,
							"Sampling percentage should be between 0 and 100"));
				}
				int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
				sample = new SplitSample(percent, seedNum);
			} else if (type.getType() == HiveASTParser.TOK_ROWCOUNT) {
				sample = new SplitSample(Integer.parseInt(value));
			} else {
				assert type.getType() == HiveASTParser.TOK_LENGTH;
				assertCombineInputFormat(numerator, "Total Length");
				long length = Integer.parseInt(value.substring(0, value.length() - 1));
				char last = value.charAt(value.length() - 1);
				if (last == 'k' || last == 'K') {
					length <<= 10;
				} else if (last == 'm' || last == 'M') {
					length <<= 20;
				} else if (last == 'g' || last == 'G') {
					length <<= 30;
				}
				int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
				sample = new SplitSample(length, seedNum);
			}
			String aliasId = getAliasId(alias, qb);
			nameToSplitSample.put(aliasId, sample);
		}
		// Insert this map into the stats
		qb.setTabAlias(alias, tabIdName);
		if (qb.isInsideView()) {
			qb.getAliasInsideView().add(alias.toLowerCase());
		}
		qb.addAlias(alias);

		qb.getParseInfo().setSrcForAlias(alias, tableTree);

		// if alias to CTE contains the table name, we do not do the translation because
		// cte is actually a subquery.
		if (!this.aliasToCTEs.containsKey(tabIdName)) {
			unparseTranslator.addTableNameTranslation(tableTree, SessionState.get().getCurrentDatabase());
			if (aliasIndex != 0) {
				unparseTranslator.addIdentifierTranslation((ASTNode) tabref.getChild(aliasIndex));
			}
		}

		return alias;
	}

	Map<String, SplitSample> getNameToSplitSampleMap() {
		return this.nameToSplitSample;
	}

	/**
	 * Generate a temp table out of a values clause.
	 * See also {@link #preProcessForInsert(ASTNode, HiveParserQB)}
	 */
	private ASTNode genValuesTempTable(ASTNode originalFrom, HiveParserQB qb) throws SemanticException {
		Path dataDir = null;
		if (!qb.getEncryptedTargetTablePaths().isEmpty()) {
			//currently only Insert into T values(...) is supported thus only 1 values clause
			//and only 1 target table are possible.  If/when support for
			//select ... from values(...) is added an insert statement may have multiple
			//encrypted target tables.
			dataDir = ctx.getMRTmpPath(qb.getEncryptedTargetTablePaths().get(0).toUri());
		}
		// Pick a name for the table
		SessionState ss = SessionState.get();
		String tableName = VALUES_TMP_TABLE_NAME_PREFIX + ss.getNextValuesTempTableSuffix();

		// Step 1, parse the values clause we were handed
		List<? extends Node> fromChildren = originalFrom.getChildren();
		// First child should be the virtual table ref
		ASTNode virtualTableRef = (ASTNode) fromChildren.get(0);
		assert virtualTableRef.getToken().getType() == HiveASTParser.TOK_VIRTUAL_TABREF :
				"Expected first child of TOK_VIRTUAL_TABLE to be TOK_VIRTUAL_TABREF but was " +
						virtualTableRef.getName();

		List<? extends Node> virtualTableRefChildren = virtualTableRef.getChildren();
		// First child of this should be the table name.  If it's anonymous,
		// then we don't have a table name.
		ASTNode tabName = (ASTNode) virtualTableRefChildren.get(0);
		if (tabName.getToken().getType() != HiveASTParser.TOK_ANONYMOUS) {
			// TODO, if you want to make select ... from (values(...) as foo(...) work,
			// you need to parse this list of columns names and build it into the table
			throw new SemanticException(ErrorMsg.VALUES_TABLE_CONSTRUCTOR_NOT_SUPPORTED.getMsg());
		}

		// The second child of the TOK_VIRTUAL_TABLE should be TOK_VALUES_TABLE
		ASTNode valuesTable = (ASTNode) fromChildren.get(1);
		assert valuesTable.getToken().getType() == HiveASTParser.TOK_VALUES_TABLE :
				"Expected second child of TOK_VIRTUAL_TABLE to be TOK_VALUE_TABLE but was " +
						valuesTable.getName();
		// Each of the children of TOK_VALUES_TABLE will be a TOK_VALUE_ROW
		List<? extends Node> valuesTableChildren = valuesTable.getChildren();

		// Now that we're going to start reading through the rows, open a file to write the rows too
		// If we leave this method before creating the temporary table we need to be sure to clean up
		// this file.
		Path tablePath = null;
		FileSystem fs = null;
		FSDataOutputStream out = null;
		try {
			if (dataDir == null) {
				tablePath = Warehouse.getDnsPath(new Path(ss.getTempTableSpace(), tableName), conf);
			} else {
				//if target table of insert is encrypted, make sure temporary table data is stored
				//similarly encrypted
				tablePath = Warehouse.getDnsPath(new Path(dataDir, tableName), conf);
			}
			fs = tablePath.getFileSystem(conf);
			fs.mkdirs(tablePath);
			Path dataFile = new Path(tablePath, "data_file");
			out = fs.create(dataFile);
			List<FieldSchema> fields = new ArrayList<FieldSchema>();

			boolean firstRow = true;
			for (Node n : valuesTableChildren) {
				ASTNode valuesRow = (ASTNode) n;
				assert valuesRow.getToken().getType() == HiveASTParser.TOK_VALUE_ROW :
						"Expected child of TOK_VALUE_TABLE to be TOK_VALUE_ROW but was " + valuesRow.getName();
				// Each of the children of this should be a literal
				List<? extends Node> valuesRowChildren = valuesRow.getChildren();
				boolean isFirst = true;
				int nextColNum = 1;
				for (Node n1 : valuesRowChildren) {
					ASTNode value = (ASTNode) n1;
					if (firstRow) {
						fields.add(new FieldSchema("tmp_values_col" + nextColNum++, "string", ""));
					}
					if (isFirst) {
						isFirst = false;
					} else {
						HiveParserUtils.writeAsText("\u0001", out);
					}
					HiveParserUtils.writeAsText(unparseExprForValuesClause(value), out);
				}
				HiveParserUtils.writeAsText("\n", out);
				firstRow = false;
			}

			// Step 2, create a temp table, using the created file as the data
			StorageFormat format = new StorageFormat(conf);
			format.processStorageFormat("TextFile");
			Table table = db.newTable(tableName);
			table.setSerializationLib(format.getSerde());
			table.setFields(fields);
			table.setDataLocation(tablePath);
			table.getTTable().setTemporary(true);
			table.setStoredAsSubDirectories(false);
			table.setInputFormatClass(format.getInputFormat());
			table.setOutputFormatClass(format.getOutputFormat());
			db.createTable(table, false);
		} catch (Exception e) {
			String errMsg = ErrorMsg.INSERT_CANNOT_CREATE_TEMP_FILE.getMsg() + e.getMessage();
			LOG.error(errMsg);
			// Try to delete the file
			if (fs != null && tablePath != null) {
				try {
					fs.delete(tablePath, false);
				} catch (IOException swallowIt) {
				}
			}
			throw new SemanticException(errMsg, e);
		} finally {
			IOUtils.closeStream(out);
		}

		// Step 3, return a new subtree with a from clause built around that temp table
		// The form of the tree is TOK_TABREF->TOK_TABNAME->identifier(tablename)
		Token t = new ClassicToken(HiveASTParser.TOK_TABREF);
		ASTNode tabRef = new ASTNode(t);
		t = new ClassicToken(HiveASTParser.TOK_TABNAME);
		ASTNode tabNameNode = new ASTNode(t);
		tabRef.addChild(tabNameNode);
		t = new ClassicToken(HiveASTParser.Identifier, tableName);
		ASTNode identifier = new ASTNode(t);
		tabNameNode.addChild(identifier);
		return tabRef;
	}

	// Take an expression in the values clause and turn it back into a string.  This is far from
	// comprehensive.  At the moment it only supports:
	// * literals (all types)
	// * unary negatives
	// * true/false
	private String unparseExprForValuesClause(ASTNode expr) throws SemanticException {
		switch (expr.getToken().getType()) {
			case HiveASTParser.Number:
				return expr.getText();

			case HiveASTParser.StringLiteral:
				return unescapeSQLString(expr.getText());

			case HiveASTParser.KW_FALSE:
				// UDFToBoolean casts any non-empty string to true, so set this to false
				return "";

			case HiveASTParser.KW_TRUE:
				return "TRUE";

			case HiveASTParser.MINUS:
				return "-" + unparseExprForValuesClause((ASTNode) expr.getChildren().get(0));

			case HiveASTParser.TOK_NULL:
				// Hive's text input will translate this as a null
				return "\\N";

			default:
				throw new SemanticException("Expression of type " + expr.getText() +
						" not supported in insert/values");
		}

	}

	private void assertCombineInputFormat(Tree numerator, String message) throws SemanticException {
		String inputFormat = conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez") ?
				HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZINPUTFORMAT) :
				HiveConf.getVar(conf, HiveConf.ConfVars.HIVEINPUTFORMAT);
		if (!inputFormat.equals(CombineHiveInputFormat.class.getName())) {
			throw new SemanticException(HiveParserUtils.generateErrorMessage((ASTNode) numerator,
					message + " sampling is not supported in " + inputFormat));
		}
	}

	private String processSubQuery(HiveParserQB qb, ASTNode subq) throws SemanticException {

		// This is a subquery and must have an alias
		if (subq.getChildCount() != 2) {
			throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(subq));
		}
		ASTNode subqref = (ASTNode) subq.getChild(0);
		String alias = unescapeIdentifier(subq.getChild(1).getText());

		// Recursively do the first phase of semantic analysis for the subquery
		HiveParserQBExpr qbexpr = new HiveParserQBExpr(alias);

		doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias, qb.isInsideView());

		// If the alias is already there then we have a conflict
		if (qb.exists(alias)) {
			throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq
					.getChild(1)));
		}
		// Insert this map into the stats
		qb.setSubqAlias(alias, qbexpr);
		qb.addAlias(alias);

		unparseTranslator.addIdentifierTranslation((ASTNode) subq.getChild(1));

		return alias;
	}

	/*
	 * Phase1: hold onto any CTE definitions in aliasToCTE.
	 * CTE definitions are global to the Query.
	 */
	private void processCTE(HiveParserQB qb, ASTNode ctes) throws SemanticException {

		int numCTEs = ctes.getChildCount();

		for (int i = 0; i < numCTEs; i++) {
			ASTNode cte = (ASTNode) ctes.getChild(i);
			ASTNode cteQry = (ASTNode) cte.getChild(0);
			String alias = unescapeIdentifier(cte.getChild(1).getText());

			String qName = qb.getId() == null ? "" : qb.getId() + ":";
			qName += alias.toLowerCase();

			if (aliasToCTEs.containsKey(qName)) {
				throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(cte.getChild(1)));
			}
			aliasToCTEs.put(qName, new CTEClause(qName, cteQry));
		}
	}

	/*
	 * We allow CTE definitions in views. So we can end up with a hierarchy of CTE definitions:
	 * - at the top level of a query statement
	 * - where a view is referenced.
	 * - views may refer to other views.
	 *
	 * The scoping rules we use are: to search for a CTE from the current HiveParserQB outwards. In order to
	 * disambiguate between CTES are different levels we qualify(prefix) them with the id of the HiveParserQB
	 * they appear in when adding them to the <code>aliasToCTEs</code> map.
	 *
	 */
	private CTEClause findCTEFromName(HiveParserQB qb, String cteName) {
		StringBuilder qId = new StringBuilder();
		if (qb.getId() != null) {
			qId.append(qb.getId());
		}

		while (qId.length() > 0) {
			String nm = qId + ":" + cteName;
			CTEClause cte = aliasToCTEs.get(nm);
			if (cte != null) {
				return cte;
			}
			int lastIndex = qId.lastIndexOf(":");
			lastIndex = lastIndex < 0 ? 0 : lastIndex;
			qId.setLength(lastIndex);
		}
		return aliasToCTEs.get(cteName);
	}

	/*
	 * If a CTE is referenced in a QueryBlock:
	 * - add it as a SubQuery for now.
	 *   - SQ.alias is the alias used in HiveParserQB. (if no alias is specified,
	 *     it used the CTE name. Works just like table references)
	 *   - Adding SQ done by:
	 *     - copying AST of CTE
	 *     - setting ASTOrigin on cloned AST.
	 *   - trigger phase 1 on new HiveParserQBExpr.
	 *   - update HiveParserQB data structs: remove this as a table reference, move it to a SQ invocation.
	 */
	private void addCTEAsSubQuery(HiveParserQB qb, String cteName, String cteAlias)
			throws SemanticException {
		cteAlias = cteAlias == null ? cteName : cteAlias;
		CTEClause cte = findCTEFromName(qb, cteName);
		ASTNode cteQryNode = cte.cteNode;
		HiveParserQBExpr cteQBExpr = new HiveParserQBExpr(cteAlias);
		doPhase1QBExpr(cteQryNode, cteQBExpr, qb.getId(), cteAlias);
		qb.rewriteCTEToSubq(cteAlias, cteName, cteQBExpr);
	}

	private final CTEClause rootClause = new CTEClause(null, null);

	/**
	 * Given the AST with TOK_JOIN as the root, get all the aliases for the tables
	 * or subqueries in the join.
	 *
	 * @param qb
	 * @param join
	 * @throws SemanticException
	 */
	@SuppressWarnings("nls")
	private void processJoin(HiveParserQB qb, ASTNode join) throws SemanticException {
		int numChildren = join.getChildCount();
		if ((numChildren != 2) && (numChildren != 3)
				&& join.getToken().getType() != HiveASTParser.TOK_UNIQUEJOIN) {
			throw new SemanticException(HiveParserUtils.generateErrorMessage(join,
					"Join with multiple children"));
		}

		queryProperties.incrementJoinCount(HiveParserUtils.isOuterJoinToken(join));
		for (int num = 0; num < numChildren; num++) {
			ASTNode child = (ASTNode) join.getChild(num);
			if (child.getToken().getType() == HiveASTParser.TOK_TABREF) {
				processTable(qb, child);
			} else if (child.getToken().getType() == HiveASTParser.TOK_SUBQUERY) {
				processSubQuery(qb, child);
			} else if (child.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
				queryProperties.setHasPTF(true);
				processPTF(qb, child);
				HiveParserPTFInvocationSpec ptfInvocationSpec = qb.getPTFInvocationSpec(child);
				String inputAlias = ptfInvocationSpec == null ? null : ptfInvocationSpec.getFunction().getAlias();
				if (inputAlias == null) {
					throw new SemanticException(HiveParserUtils.generateErrorMessage(child,
							"PTF invocation in a Join must have an alias"));
				}

			} else if (child.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW ||
					child.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW_OUTER) {
				// SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
				// is not supported. Instead, the lateral view must be in a subquery
				// SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
				// JOIN src2 ...
				throw new SemanticException(ErrorMsg.LATERAL_VIEW_WITH_JOIN
						.getMsg(join));
			} else if (HiveParserUtils.isJoinToken(child)) {
				processJoin(qb, child);
			}
		}
	}

	/**
	 * Given the AST with TOK_LATERAL_VIEW as the root, get the alias for the
	 * table or subquery in the lateral view and also make a mapping from the
	 * alias to all the lateral view AST's.
	 *
	 * @param qb
	 * @param lateralView
	 * @return the alias for the table/subquery
	 * @throws SemanticException
	 */

	private String processLateralView(HiveParserQB qb, ASTNode lateralView)
			throws SemanticException {
		int numChildren = lateralView.getChildCount();

		assert (numChildren == 2);
		ASTNode next = (ASTNode) lateralView.getChild(1);

		String alias = null;

		switch (next.getToken().getType()) {
			case HiveASTParser.TOK_TABREF:
				alias = processTable(qb, next);
				break;
			case HiveASTParser.TOK_SUBQUERY:
				alias = processSubQuery(qb, next);
				break;
			case HiveASTParser.TOK_LATERAL_VIEW:
			case HiveASTParser.TOK_LATERAL_VIEW_OUTER:
				alias = processLateralView(qb, next);
				break;
			default:
				throw new SemanticException(ErrorMsg.LATERAL_VIEW_INVALID_CHILD
						.getMsg(lateralView));
		}
		alias = alias.toLowerCase();
		qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
		qb.addAlias(alias);
		return alias;
	}

	/**
	 * Phase 1: (including, but not limited to):
	 * 1. Gets all the aliases for all the tables / subqueries and makes the
	 * appropriate mapping in aliasToTabs, aliasToSubq 2. Gets the location of the
	 * destination and names the clause "inclause" + i 3. Creates a map from a
	 * string representation of an aggregation tree to the actual aggregation AST
	 * 4. Creates a mapping from the clause name to the select expression AST in
	 * destToSelExpr 5. Creates a mapping from a table alias to the lateral view
	 * AST's in aliasToLateralViews
	 */
	@SuppressWarnings({"fallthrough", "nls"})
	public boolean doPhase1(ASTNode ast, HiveParserQB qb, Phase1Ctx ctx1, HiveParserPlannerContext plannerCtx) throws SemanticException {

		boolean phase1Result = true;
		HiveParserQBParseInfo qbp = qb.getParseInfo();
		boolean skipRecursion = false;

		if (ast.getToken() != null) {
			skipRecursion = true;
			switch (ast.getToken().getType()) {
				case HiveASTParser.TOK_SELECTDI:
					qb.countSelDi();
					// fall through
				case HiveASTParser.TOK_SELECT:
					qb.countSel();
					qbp.setSelExprForClause(ctx1.dest, ast);

					int posn = 0;
					if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveASTParser.QUERY_HINT) {
						HiveASTParseDriver pd = new HiveASTParseDriver();
						String queryHintStr = ast.getChild(0).getText();
						if (LOG.isDebugEnabled()) {
							LOG.debug("QUERY HINT: " + queryHintStr);
						}
						try {
							ASTNode hintNode = pd.parseHint(queryHintStr);
							qbp.setHints(hintNode);
							posn++;
						} catch (ParseException e) {
							throw new SemanticException("failed to parse query hint: " + e.getMessage(), e);
						}
					}

					if ((ast.getChild(posn).getChild(0).getType() == HiveASTParser.TOK_TRANSFORM)) {
						queryProperties.setUsesScript(true);
					}

					LinkedHashMap<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast, qb, ctx1.dest);
					doPhase1GetColumnAliasesFromSelect(ast, qbp);
					qbp.setAggregationExprsForClause(ctx1.dest, aggregations);
					qbp.setDistinctFuncExprsForClause(ctx1.dest, doPhase1GetDistinctFuncExprs(aggregations));
					break;

				case HiveASTParser.TOK_WHERE:
					qbp.setWhrExprForClause(ctx1.dest, ast);
					if (!HiveParserSubQueryUtils.findSubQueries((ASTNode) ast.getChild(0)).isEmpty()) {
						queryProperties.setFilterWithSubQuery(true);
					}
					break;

				case HiveASTParser.TOK_INSERT_INTO:
					String currentDatabase = SessionState.get().getCurrentDatabase();
					String tabName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0), currentDatabase);
					qbp.addInsertIntoTable(tabName, ast);
					// TODO: hive doesn't break here, so we copy what's below here
					handleTokDestination(ctx1, ast, qbp, plannerCtx);
					break;

				case HiveASTParser.TOK_DESTINATION:
					handleTokDestination(ctx1, ast, qbp, plannerCtx);
					break;

				case HiveASTParser.TOK_FROM:
					int childCount = ast.getChildCount();
					if (childCount != 1) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast, "Multiple Children " + childCount));
					}

					if (!qbp.getIsSubQ()) {
						qbp.setQueryFromExpr(ast);
					}

					// Check if this is a subquery / lateral view
					ASTNode frm = (ASTNode) ast.getChild(0);
					if (frm.getToken().getType() == HiveASTParser.TOK_TABREF) {
						processTable(qb, frm);
					} else if (frm.getToken().getType() == HiveASTParser.TOK_VIRTUAL_TABLE) {
						// Create a temp table with the passed values in it then rewrite this portion of the
						// tree to be from that table.
						ASTNode newFrom = genValuesTempTable(frm, qb);
						ast.setChild(0, newFrom);
						processTable(qb, newFrom);
					} else if (frm.getToken().getType() == HiveASTParser.TOK_SUBQUERY) {
						processSubQuery(qb, frm);
					} else if (frm.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW ||
							frm.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW_OUTER) {
						queryProperties.setHasLateralViews(true);
						processLateralView(qb, frm);
					} else if (HiveParserUtils.isJoinToken(frm)) {
						processJoin(qb, frm);
						qbp.setJoinExpr(frm);
					} else if (frm.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
						queryProperties.setHasPTF(true);
						processPTF(qb, frm);
					}
					break;

				case HiveASTParser.TOK_CLUSTERBY:
					// Get the clusterby aliases - these are aliased to the entries in the
					// select list
					queryProperties.setHasClusterBy(true);
					qbp.setClusterByExprForClause(ctx1.dest, ast);
					break;

				case HiveASTParser.TOK_DISTRIBUTEBY:
					// Get the distribute by aliases - these are aliased to the entries in the select list
					queryProperties.setHasDistributeBy(true);
					qbp.setDistributeByExprForClause(ctx1.dest, ast);
					if (qbp.getClusterByForClause(ctx1.dest) != null) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
					} else if (qbp.getOrderByForClause(ctx1.dest) != null) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
					}
					break;

				case HiveASTParser.TOK_SORTBY:
					// Get the sort by aliases - these are aliased to the entries in the
					// select list
					queryProperties.setHasSortBy(true);
					qbp.setSortByExprForClause(ctx1.dest, ast);
					if (qbp.getClusterByForClause(ctx1.dest) != null) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg()));
					} else if (qbp.getOrderByForClause(ctx1.dest) != null) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg()));
					}
					break;

				case HiveASTParser.TOK_ORDERBY:
					// Get the order by aliases - these are aliased to the entries in the
					// select list
					queryProperties.setHasOrderBy(true);
					qbp.setOrderByExprForClause(ctx1.dest, ast);
					if (qbp.getClusterByForClause(ctx1.dest) != null) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg()));
					}
					break;

				case HiveASTParser.TOK_GROUPBY:
				case HiveASTParser.TOK_ROLLUP_GROUPBY:
				case HiveASTParser.TOK_CUBE_GROUPBY:
				case HiveASTParser.TOK_GROUPING_SETS:
					// Get the groupby aliases - these are aliased to the entries in the
					// select list
					queryProperties.setHasGroupBy(true);
					if (qbp.getJoinExpr() != null) {
						queryProperties.setHasJoinFollowedByGroupBy(true);
					}
					if (qbp.getSelForClause(ctx1.dest).getToken().getType() == HiveASTParser.TOK_SELECTDI) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg()));
					}
					qbp.setGroupByExprForClause(ctx1.dest, ast);
					skipRecursion = true;

					// Rollup and Cubes are syntactic sugar on top of grouping sets
					if (ast.getToken().getType() == HiveASTParser.TOK_ROLLUP_GROUPBY) {
						qbp.getDestRollups().add(ctx1.dest);
					} else if (ast.getToken().getType() == HiveASTParser.TOK_CUBE_GROUPBY) {
						qbp.getDestCubes().add(ctx1.dest);
					} else if (ast.getToken().getType() == HiveASTParser.TOK_GROUPING_SETS) {
						qbp.getDestGroupingSets().add(ctx1.dest);
					}
					break;

				case HiveASTParser.TOK_HAVING:
					qbp.setHavingExprForClause(ctx1.dest, ast);
					qbp.addAggregationExprsForClause(ctx1.dest,
							doPhase1GetAggregationsFromSelect(ast, qb, ctx1.dest));
					break;

				case HiveASTParser.KW_WINDOW:
					if (!qb.hasWindowingSpec(ctx1.dest)) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
								"Query has no Cluster/Distribute By; but has a Window definition"));
					}
					handleQueryWindowClauses(qb, ctx1, ast);
					break;

				case HiveASTParser.TOK_LIMIT:
					if (ast.getChildCount() == 2) {
						qbp.setDestLimit(ctx1.dest,
								new Integer(ast.getChild(0).getText()),
								new Integer(ast.getChild(1).getText()));
					} else {
						qbp.setDestLimit(ctx1.dest, 0, new Integer(ast.getChild(0).getText()));
					}
					break;

				case HiveASTParser.TOK_ANALYZE:
					// Case of analyze command
					String tableName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0)).toLowerCase();

					qb.setTabAlias(tableName, tableName);
					qb.addAlias(tableName);
					qb.getParseInfo().setIsAnalyzeCommand(true);
					qb.getParseInfo().setNoScanAnalyzeCommand(this.noscan);
					qb.getParseInfo().setPartialScanAnalyzeCommand(this.partialscan);
					// Allow analyze the whole table and dynamic partitions
					HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
					HiveConf.setVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");

					break;

				case HiveASTParser.TOK_UNIONALL:
					if (!qbp.getIsSubQ()) {
						// this shouldn't happen. The parser should have converted the union to be
						// contained in a subquery. Just in case, we keep the error as a fallback.
						throw new SemanticException(HiveParserUtils.generateErrorMessage(ast, ErrorMsg.UNION_NOTIN_SUBQ.getMsg()));
					}
					skipRecursion = false;
					break;

				case HiveASTParser.TOK_INSERT:
					ASTNode destination = (ASTNode) ast.getChild(0);
					Tree tab = destination.getChild(0);

					// Proceed if AST contains partition & If Not Exists
					if (destination.getChildCount() == 2 &&
							tab.getChildCount() == 2 &&
							destination.getChild(1).getType() == HiveASTParser.TOK_IFNOTEXISTS) {
						String name = tab.getChild(0).getChild(0).getText();

						Tree partitions = tab.getChild(1);
						int numChildren = partitions.getChildCount();
						HashMap<String, String> partition = new HashMap<>();
						for (int i = 0; i < numChildren; i++) {
							String partitionName = partitions.getChild(i).getChild(0).getText();
							Tree pvalue = partitions.getChild(i).getChild(1);
							if (pvalue == null) {
								break;
							}
							String partitionVal = stripQuotes(pvalue.getText());
							partition.put(partitionName, partitionVal);
						}
						// if it is a dynamic partition throw the exception
						if (numChildren != partition.size()) {
							throw new SemanticException(ErrorMsg.INSERT_INTO_DYNAMICPARTITION_IFNOTEXISTS
									.getMsg(partition.toString()));
						}
						Table table = null;
						try {
							table = getTableObjectByName(name);
						} catch (HiveException ex) {
							throw new SemanticException(ex);
						}
						try {
							Partition parMetaData = db.getPartition(table, partition, false);
							// Check partition exists if it exists skip the overwrite
							if (parMetaData != null) {
								phase1Result = false;
								skipRecursion = true;
								LOG.info("Partition already exists so insert into overwrite " +
										"skipped for partition : " + parMetaData.toString());
								break;
							}
						} catch (HiveException e) {
							LOG.info("Error while getting metadata : ", e);
						}
						validatePartSpec(table, partition, (ASTNode) tab, conf, false);
					}
					skipRecursion = false;
					break;
				case HiveASTParser.TOK_LATERAL_VIEW:
				case HiveASTParser.TOK_LATERAL_VIEW_OUTER:
					// todo: nested LV
					assert ast.getChildCount() == 1;
					qb.getParseInfo().getDestToLateralView().put(ctx1.dest, ast);
					break;
				case HiveASTParser.TOK_CTE:
					processCTE(qb, ast);
					break;
				default:
					skipRecursion = false;
					break;
			}
		}

		if (!skipRecursion) {
			// Iterate over the rest of the children
			int childCount = ast.getChildCount();
			for (int childPos = 0; childPos < childCount && phase1Result; ++childPos) {
				phase1Result = doPhase1((ASTNode) ast.getChild(childPos), qb, ctx1, plannerCtx);
			}
		}
		return phase1Result;
	}

	private void handleTokDestination(Phase1Ctx ctx1, ASTNode ast, HiveParserQBParseInfo qbp, HiveParserPlannerContext plannerCtx)
			throws SemanticException {
		ctx1.dest = this.ctx.getDestNamePrefix(ast).toString() + ctx1.nextNum;
		ctx1.nextNum++;
		boolean isTmpFileDest = false;
		if (ast.getChildCount() > 0 && ast.getChild(0) instanceof ASTNode) {
			ASTNode ch = (ASTNode) ast.getChild(0);
			if (ch.getToken().getType() == HiveASTParser.TOK_DIR && ch.getChildCount() > 0
					&& ch.getChild(0) instanceof ASTNode) {
				ch = (ASTNode) ch.getChild(0);
				isTmpFileDest = ch.getToken().getType() == HiveASTParser.TOK_TMP_FILE;
			} else {
				if (ast.getToken().getType() == HiveASTParser.TOK_DESTINATION
						&& ast.getChild(0).getType() == HiveASTParser.TOK_TAB) {
					String fullTableName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0),
							SessionState.get().getCurrentDatabase());
					qbp.getInsertOverwriteTables().put(fullTableName, ast);
				}
			}
		}

		// is there a insert in the subquery
		if (qbp.getIsSubQ() && !isTmpFileDest) {
			throw new SemanticException(ErrorMsg.NO_INSERT_INSUBQUERY.getMsg(ast));
		}

		qbp.setDestForClause(ctx1.dest, (ASTNode) ast.getChild(0));
		handleInsertStatementSpecPhase1(ast, qbp, ctx1);

		if (qbp.getClauseNamesForDest().size() == 2) {
			// From the moment that we have two destination clauses,
			// we know that this is a multi-insert query.
			// Thus, set property to right value.
			// Using qbp.getClauseNamesForDest().size() >= 2 would be
			// equivalent, but we use == to avoid setting the property
			// multiple times
			queryProperties.setMultiDestQuery(true);
		}

		if (plannerCtx != null && !queryProperties.hasMultiDestQuery()) {
			plannerCtx.setInsertToken(ast, isTmpFileDest);
		} else if (plannerCtx != null && qbp.getClauseNamesForDest().size() == 2) {
			// For multi-insert query, currently we only optimize the FROM clause.
			// Hence, introduce multi-insert token on top of it.
			// However, first we need to reset existing token (insert).
			// Using qbp.getClauseNamesForDest().size() >= 2 would be
			// equivalent, but we use == to avoid setting the property
			// multiple times
			plannerCtx.resetToken();
			plannerCtx.setMultiInsertToken((ASTNode) qbp.getQueryFrom().getChild(0));
		}
	}

	/**
	 * This is phase1 of supporting specifying schema in insert statement.
	 * insert into foo(z,y) select a,b from bar;
	 */
	private void handleInsertStatementSpecPhase1(ASTNode ast, HiveParserQBParseInfo qbp, Phase1Ctx ctx1) throws SemanticException {
		ASTNode tabColName = (ASTNode) ast.getChild(1);
		if (ast.getType() == HiveASTParser.TOK_INSERT_INTO && tabColName != null && tabColName.getType() == HiveASTParser.TOK_TABCOLNAME) {
			//we have "insert into foo(a,b)..."; parser will enforce that 1+ columns are listed if TOK_TABCOLNAME is present
			List<String> targetColNames = new ArrayList<String>();
			for (Node col : tabColName.getChildren()) {
				assert ((ASTNode) col).getType() == HiveASTParser.Identifier :
						"expected token " + HiveASTParser.Identifier + " found " + ((ASTNode) col).getType();
				targetColNames.add(((ASTNode) col).getText());
			}
			String fullTableName = getUnescapedName((ASTNode) ast.getChild(0).getChild(0),
					SessionState.get().getCurrentDatabase());
			qbp.setDestSchemaForClause(ctx1.dest, targetColNames);
			Set<String> targetColumns = new HashSet<String>();
			targetColumns.addAll(targetColNames);
			if (targetColNames.size() != targetColumns.size()) {
				throw new SemanticException(HiveParserUtils.generateErrorMessage(tabColName,
						"Duplicate column name detected in " + fullTableName + " table schema specification"));
			}
			Table targetTable = null;
			try {
				targetTable = db.getTable(fullTableName, false);
			} catch (HiveException ex) {
				LOG.error("Error processing HiveASTParser.TOK_DESTINATION: " + ex.getMessage(), ex);
				throw new SemanticException(ex);
			}
			if (targetTable == null) {
				throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
						"Unable to access metadata for table " + fullTableName));
			}
			for (FieldSchema f : targetTable.getCols()) {
				//parser only allows foo(a,b), not foo(foo.a, foo.b)
				targetColumns.remove(f.getName());
			}
			if (!targetColumns.isEmpty()) {//here we need to see if remaining columns are dynamic partition columns
            /* We just checked the user specified schema columns among regular table column and found some which are not
            'regular'.  Now check is they are dynamic partition columns
              For dynamic partitioning,
              Given "create table multipart(a int, b int) partitioned by (c int, d int);"
              for "insert into multipart partition(c='1',d)(d,a) values(2,3);" we expect parse tree to look like this
               (TOK_INSERT_INTO
                (TOK_TAB
                  (TOK_TABNAME multipart)
                  (TOK_PARTSPEC
                    (TOK_PARTVAL c '1')
                    (TOK_PARTVAL d)
                  )
                )
                (TOK_TABCOLNAME d a)
               )*/
				List<String> dynamicPartitionColumns = new ArrayList<String>();
				if (ast.getChild(0) != null && ast.getChild(0).getType() == HiveASTParser.TOK_TAB) {
					ASTNode tokTab = (ASTNode) ast.getChild(0);
					ASTNode tokPartSpec = (ASTNode) tokTab.getFirstChildWithType(HiveASTParser.TOK_PARTSPEC);
					if (tokPartSpec != null) {
						for (Node n : tokPartSpec.getChildren()) {
							ASTNode tokPartVal = null;
							if (n instanceof ASTNode) {
								tokPartVal = (ASTNode) n;
							}
							if (tokPartVal != null && tokPartVal.getType() == HiveASTParser.TOK_PARTVAL && tokPartVal.getChildCount() == 1) {
								assert tokPartVal.getChild(0).getType() == HiveASTParser.Identifier :
										"Expected column name; found tokType=" + tokPartVal.getType();
								dynamicPartitionColumns.add(tokPartVal.getChild(0).getText());
							}
						}
					}
				}
				for (String colName : dynamicPartitionColumns) {
					targetColumns.remove(colName);
				}
				if (!targetColumns.isEmpty()) {
					// Found some columns in user specified schema which are neither regular not dynamic partition columns
					throw new SemanticException(HiveParserUtils.generateErrorMessage(tabColName,
							"'" + (targetColumns.size() == 1 ? targetColumns.iterator().next() : targetColumns) +
									"' in insert schema specification " + (targetColumns.size() == 1 ? "is" : "are") +
									" not found among regular columns of " +
									fullTableName + " nor dynamic partition columns."));
				}
			}
		}
	}

	public void getMaterializationMetadata(HiveParserQB qb) throws SemanticException {
		try {
			gatherCTEReferences(qb, rootClause);
			int threshold = Integer.parseInt(conf.get("hive.optimize.cte.materialize.threshold", "-1"));
			for (CTEClause cte : new HashSet<>(aliasToCTEs.values())) {
				if (threshold >= 0 && cte.reference >= threshold) {
					cte.materialize = true;
				}
			}
		} catch (HiveException e) {
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
			if (e instanceof SemanticException) {
				throw (SemanticException) e;
			}
			throw new SemanticException(e.getMessage(), e);
		}
	}

	private void gatherCTEReferences(HiveParserQBExpr qbexpr, CTEClause parent) throws HiveException {
		if (qbexpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
			gatherCTEReferences(qbexpr.getQB(), parent);
		} else {
			gatherCTEReferences(qbexpr.getQBExpr1(), parent);
			gatherCTEReferences(qbexpr.getQBExpr2(), parent);
		}
	}

	// TODO: check view references, too
	private void gatherCTEReferences(HiveParserQB qb, CTEClause current) throws HiveException {
		for (String alias : qb.getTabAliases()) {
			String tabName = qb.getTabNameForAlias(alias);
			String cteName = tabName.toLowerCase();

			CTEClause cte = findCTEFromName(qb, cteName);
			if (cte != null) {
				if (ctesExpanded.contains(cteName)) {
					throw new SemanticException("Recursive cte " + cteName +
							" detected (cycle: " + StringUtils.join(ctesExpanded, " -> ") +
							" -> " + cteName + ").");
				}
				cte.reference++;
				current.parents.add(cte);
				if (cte.qbExpr != null) {
					continue;
				}
				cte.qbExpr = new HiveParserQBExpr(cteName);
				doPhase1QBExpr(cte.cteNode, cte.qbExpr, qb.getId(), cteName);

				ctesExpanded.add(cteName);
				gatherCTEReferences(cte.qbExpr, cte);
				ctesExpanded.remove(ctesExpanded.size() - 1);
			}
		}
		for (String alias : qb.getSubqAliases()) {
			gatherCTEReferences(qb.getSubqForAlias(alias), current);
		}
	}

	public void getMetaData(HiveParserQB qb) throws SemanticException {
		getMetaData(qb, false);
	}

	public void getMetaData(HiveParserQB qb, boolean enableMaterialization) throws SemanticException {
		try {
			if (enableMaterialization) {
				getMaterializationMetadata(qb);
			}
			getMetaData(qb, null);
		} catch (HiveException e) {
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
			if (e instanceof SemanticException) {
				throw (SemanticException) e;
			}
			throw new SemanticException(e.getMessage(), e);
		}
	}

	private void getMetaData(HiveParserQBExpr qbexpr, ReadEntity parentInput)
			throws HiveException {
		if (qbexpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
			getMetaData(qbexpr.getQB(), parentInput);
		} else {
			getMetaData(qbexpr.getQBExpr1(), parentInput);
			getMetaData(qbexpr.getQBExpr2(), parentInput);
		}
	}

	@SuppressWarnings("nls")
	private void getMetaData(HiveParserQB qb, ReadEntity parentInput)
			throws HiveException {
		LOG.info("Get metadata for source tables");

		// Go over the tables and populate the related structures. We have to materialize the table alias list since we might
		// modify it in the middle for view rewrite.
		List<String> tabAliases = new ArrayList<>(qb.getTabAliases());

		// Keep track of view alias to view name and read entity
		// For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
		// keeps track of full view name and read entity corresponding to alias V3, V3:V2, V3:V2:V1.
		// This is needed for tracking the dependencies for inputs, along with their parents.
		Map<String, ObjectPair<String, ReadEntity>> aliasToViewInfo = new HashMap<>();

		// used to capture view to SQ conversions. This is used to check for recursive CTE invocations.
		Map<String, String> sqAliasToCTEName = new HashMap<>();

		for (String alias : tabAliases) {
			String tabName = qb.getTabNameForAlias(alias);
			String cteName = tabName.toLowerCase();

			Table tab = db.getTable(tabName, false);
			if (tab == null ||
					tab.getDbName().equals(SessionState.get().getCurrentDatabase())) {
//				Table materializedTab = ctx.getMaterializedTable(cteName);
				Table materializedTab = null;
				if (materializedTab == null) {
					// we first look for this alias from CTE, and then from catalog.
					CTEClause cte = findCTEFromName(qb, cteName);
					if (cte != null) {
						if (!cte.materialize) {
							addCTEAsSubQuery(qb, cteName, alias);
							sqAliasToCTEName.put(alias, cteName);
							continue;
						}
						throw new SemanticException("Materializing CTE is not supported at the moment");
//						tab = materializeCTE(cteName, cte);
					}
				} else {
					tab = materializedTab;
				}
			}

			if (tab == null) {
				ASTNode src = qb.getParseInfo().getSrcForAlias(alias);
				if (null != src) {
					throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(src));
				} else {
					throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(alias));
				}
			}
			if (tab.isView()) {
				if (qb.getParseInfo().isAnalyzeCommand()) {
					throw new SemanticException(ErrorMsg.ANALYZE_VIEW.getMsg());
				}
				String fullViewName = tab.getDbName() + "." + tab.getTableName();
				// Prevent view cycles
				if (viewsExpanded.contains(fullViewName)) {
					throw new SemanticException("Recursive view " + fullViewName +
							" detected (cycle: " + StringUtils.join(viewsExpanded, " -> ") +
							" -> " + fullViewName + ").");
				}
				replaceViewReferenceWithDefinition(qb, tab, tabName, alias);
				// This is the last time we'll see the Table objects for views, so add it to the inputs
				// now. isInsideView will tell if this view is embedded in another view.
				// If the view is Inside another view, it should have at least one parent
				if (qb.isInsideView() && parentInput == null) {
					parentInput = PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
				}
				ReadEntity viewInput = new ReadEntity(tab, parentInput, !qb.isInsideView());
				viewInput = PlanUtils.addInput(inputs, viewInput);
				aliasToViewInfo.put(alias, new ObjectPair<String, ReadEntity>(fullViewName, viewInput));
				String aliasId = getAliasId(alias, qb);
				if (aliasId != null) {
					aliasId = aliasId.replace(SUBQUERY_TAG_1, "")
							.replace(SUBQUERY_TAG_2, "");
				}
				viewAliasToInput.put(aliasId, viewInput);
				continue;
			}

			if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass())) {
				throw new SemanticException(HiveParserUtils.generateErrorMessage(
						qb.getParseInfo().getSrcForAlias(alias),
						ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg()));
			}

			qb.getMetaData().setSrcForAlias(alias, tab);

			if (qb.getParseInfo().isAnalyzeCommand()) {
				// allow partial partition specification for nonscan since noscan is fast.
				TableSpec ts = new TableSpec(db, conf, (ASTNode) ast.getChild(0), true, this.noscan);
				if (ts.specType == SpecType.DYNAMIC_PARTITION) { // dynamic partitions
					try {
						ts.partitions = db.getPartitionsByNames(ts.tableHandle, ts.partSpec);
					} catch (HiveException e) {
						throw new SemanticException(HiveParserUtils.generateErrorMessage(
								qb.getParseInfo().getSrcForAlias(alias),
								"Cannot get partitions for " + ts.partSpec), e);
					}
				}
				// validate partial scan command
				HiveParserQBParseInfo qbpi = qb.getParseInfo();
				if (qbpi.isPartialScanAnalyzeCommand()) {
					Class<? extends InputFormat> inputFormatClass = null;
					switch (ts.specType) {
						case TABLE_ONLY:
						case DYNAMIC_PARTITION:
							inputFormatClass = ts.tableHandle.getInputFormatClass();
							break;
						case STATIC_PARTITION:
							inputFormatClass = ts.partHandle.getInputFormatClass();
							break;
						default:
							assert false;
					}
					// throw a HiveException for formats other than rcfile or orcfile.
					if (!(inputFormatClass.equals(RCFileInputFormat.class) || inputFormatClass
							.equals(OrcInputFormat.class))) {
						throw new SemanticException(ErrorMsg.ANALYZE_TABLE_PARTIALSCAN_NON_RCFILE.getMsg());
					}
				}

//				tab.setTableSpec(ts);
				qb.getParseInfo().addTableSpec(alias, ts);
			}

			ReadEntity parentViewInfo = PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
			// Temporary tables created during the execution are not the input sources
			if (!HiveParserUtils.isValuesTempTable(alias)) {
				HiveParserUtils.addInput(inputs,
						new ReadEntity(tab, parentViewInfo, parentViewInfo == null), mergeIsDirect);
			}
		}

		LOG.info("Get metadata for subqueries");
		// Go over the subqueries and getMetaData for these
		for (String alias : qb.getSubqAliases()) {
			boolean wasView = aliasToViewInfo.containsKey(alias);
			boolean wasCTE = sqAliasToCTEName.containsKey(alias);
			ReadEntity newParentInput = null;
			if (wasView) {
				viewsExpanded.add(aliasToViewInfo.get(alias).getFirst());
				newParentInput = aliasToViewInfo.get(alias).getSecond();
			} else if (wasCTE) {
				ctesExpanded.add(sqAliasToCTEName.get(alias));
			}
			HiveParserQBExpr qbexpr = qb.getSubqForAlias(alias);
			getMetaData(qbexpr, newParentInput);
			if (wasView) {
				viewsExpanded.remove(viewsExpanded.size() - 1);
			} else if (wasCTE) {
				ctesExpanded.remove(ctesExpanded.size() - 1);
			}
		}

		HiveParserRowFormatParams rowFormatParams = new HiveParserRowFormatParams();
		StorageFormat storageFormat = new StorageFormat(conf);

		LOG.info("Get metadata for destination tables");
		// Go over all the destination structures and populate the related metadata
		HiveParserQBParseInfo qbp = qb.getParseInfo();

		for (String name : qbp.getClauseNamesForDest()) {
			ASTNode ast = qbp.getDestForClause(name);
			switch (ast.getToken().getType()) {
				case HiveASTParser.TOK_TAB: {
					TableSpec ts = new TableSpec(db, conf, ast);
					if (ts.tableHandle.isView() || hiveShim.isMaterializedView(ts.tableHandle)) {
						throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
					}

					Class<?> outputFormatClass = ts.tableHandle.getOutputFormatClass();
					if (!ts.tableHandle.isNonNative() &&
							!HiveOutputFormat.class.isAssignableFrom(outputFormatClass)) {
						throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
								.getMsg(ast, "The class is " + outputFormatClass.toString()));
					}

					boolean isTableWrittenTo = qb.getParseInfo().isInsertIntoTable(ts.tableHandle.getDbName(),
							ts.tableHandle.getTableName());
					isTableWrittenTo |= (qb.getParseInfo().getInsertOverwriteTables().
							get(getUnescapedName((ASTNode) ast.getChild(0), ts.tableHandle.getDbName())) != null);
					assert isTableWrittenTo :
							"Inconsistent data structure detected: we are writing to " + ts.tableHandle + " in " +
									name + " but it's not in isInsertIntoTable() or getInsertOverwriteTables()";
					// TableSpec ts is got from the query (user specified),
					// which means the user didn't specify partitions in their query,
					// but whether the table itself is partitioned is not know.
					if (ts.specType != SpecType.STATIC_PARTITION) {
						// This is a table or dynamic partition
						qb.getMetaData().setDestForAlias(name, ts.tableHandle);
						// has dynamic as well as static partitions
						if (ts.partSpec != null && ts.partSpec.size() > 0) {
							qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
						}
					} else {
						// This is a partition
						qb.getMetaData().setDestForAlias(name, ts.partHandle);
					}
					if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
						// Add the table spec for the destination table.
						qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
					}
					break;
				}

				case HiveASTParser.TOK_DIR: {
					// This is a dfs file
					String fname = stripQuotes(ast.getChild(0).getText());
					if ((!qb.getParseInfo().getIsSubQ())
							&& (((ASTNode) ast.getChild(0)).getToken().getType() == HiveASTParser.TOK_TMP_FILE)) {
						if (qb.isCTAS() || qb.isMaterializedView()) {
							qb.setIsQuery(false);
							ctx.setResDir(null);
							ctx.setResFile(null);

							Path location;
							// If the CTAS query does specify a location, use the table location, else use the db location
							if (qb.getTableDesc() != null && qb.getTableDesc().getLocation() != null) {
								location = new Path(qb.getTableDesc().getLocation());
							} else {
								// allocate a temporary output dir on the location of the table
								String tableName = getUnescapedName((ASTNode) ast.getChild(0));
								String[] names = Utilities.getDbTableName(tableName);
								try {
									Warehouse wh = new Warehouse(conf);
									//Use destination table's db location.
									String destTableDb = qb.getTableDesc() != null ? qb.getTableDesc().getDatabaseName() : null;
									if (destTableDb == null) {
										destTableDb = names[0];
									}
									location = wh.getDatabasePath(db.getDatabase(destTableDb));
								} catch (MetaException e) {
									throw new SemanticException(e);
								}
							}
							try {
								fname = ctx.getExtTmpPathRelTo(
										FileUtils.makeQualified(location, conf)).toString();
							} catch (Exception e) {
								throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
										"Error creating temporary folder on: " + location.toString()), e);
							}
							if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
								TableSpec ts = new TableSpec(db, conf, this.ast);
								// Add the table spec for the destination table.
								qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
							}
						} else {
							// This is the only place where isQuery is set to true; it defaults to false.
							qb.setIsQuery(true);
							Path stagingPath = getStagingDirectoryPathname(qb);
							fname = stagingPath.toString();
							ctx.setResDir(stagingPath);
						}
					}

					boolean isDfsFile = true;
					if (ast.getChildCount() >= 2 && ast.getChild(1).getText().toLowerCase().equals("local")) {
						isDfsFile = false;
					}
					// Set the destination for the SELECT query inside the CTAS
					qb.getMetaData().setDestForAlias(name, fname, isDfsFile);

					CreateTableDesc directoryDesc = new CreateTableDesc();
					boolean directoryDescIsSet = false;
					int numCh = ast.getChildCount();
					for (int num = 1; num < numCh; num++) {
						ASTNode child = (ASTNode) ast.getChild(num);
						if (child != null) {
							if (storageFormat.fillStorageFormat(child)) {
								directoryDesc.setOutputFormat(storageFormat.getOutputFormat());
								directoryDesc.setSerName(storageFormat.getSerde());
								directoryDescIsSet = true;
								continue;
							}
							switch (child.getToken().getType()) {
								case HiveASTParser.TOK_TABLEROWFORMAT:
									rowFormatParams.analyzeRowFormat(child);
									directoryDesc.setFieldDelim(rowFormatParams.fieldDelim);
									directoryDesc.setLineDelim(rowFormatParams.lineDelim);
									directoryDesc.setCollItemDelim(rowFormatParams.collItemDelim);
									directoryDesc.setMapKeyDelim(rowFormatParams.mapKeyDelim);
									directoryDesc.setFieldEscape(rowFormatParams.fieldEscape);
									directoryDesc.setNullFormat(rowFormatParams.nullFormat);
									directoryDescIsSet = true;
									break;
								case HiveASTParser.TOK_TABLESERIALIZER:
									ASTNode serdeChild = (ASTNode) child.getChild(0);
									storageFormat.setSerde(unescapeSQLString(serdeChild.getChild(0).getText()));
									directoryDesc.setSerName(storageFormat.getSerde());
									if (serdeChild.getChildCount() > 1) {
										directoryDesc.setSerdeProps(new HashMap<String, String>());
										readProps((ASTNode) serdeChild.getChild(1).getChild(0), directoryDesc.getSerdeProps());
									}
									directoryDescIsSet = true;
									break;
							}
						}
					}
					if (directoryDescIsSet) {
						qb.setDirectoryDesc(directoryDesc);
					}
					break;
				}
				default:
					throw new SemanticException(HiveParserUtils.generateErrorMessage(ast,
							"Unknown Token Type " + ast.getToken().getType()));
			}
		}
	}

	private Path getStagingDirectoryPathname(HiveParserQB qb) throws HiveException {
		return ctx.getMRTmpPath();
	}

	private void replaceViewReferenceWithDefinition(HiveParserQB qb, Table tab,
			String tabName, String alias) throws SemanticException {
		ASTNode viewTree;
		final ASTNodeOrigin viewOrigin = new ASTNodeOrigin("VIEW", tab.getTableName(),
				tab.getViewExpandedText(), alias, qb.getParseInfo().getSrcForAlias(
				alias));
		try {
			// Reparse text, passing null for context to avoid clobbering
			// the top-level token stream.
			String viewText = tab.getViewExpandedText();
			viewTree = HiveASTParseUtils.parse(viewText, ctx, tab.getCompleteName());

			Dispatcher nodeOriginDispatcher = (nd, stack, nodeOutputs) -> {
				((ASTNode) nd).setOrigin(viewOrigin);
				return null;
			};
			GraphWalker nodeOriginTagger = new HiveParserDefaultGraphWalker(nodeOriginDispatcher);
			nodeOriginTagger.startWalking(Collections.singleton(viewTree), null);
		} catch (ParseException e) {
			// A user could encounter this if a stored view definition contains
			// an old SQL construct which has been eliminated in a later Hive
			// version, so we need to provide full debugging info to help
			// with fixing the view definition.
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
			StringBuilder sb = new StringBuilder();
			sb.append(e.getMessage());
			ErrorMsg.renderOrigin(sb, viewOrigin);
			throw new SemanticException(sb.toString(), e);
		}
		HiveParserQBExpr qbexpr = new HiveParserQBExpr(alias);
		doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias, true);
		// if skip authorization, skip checking;
		// if it is inside a view, skip checking;
		// if authorization flag is not enabled, skip checking.
		// if HIVE_STATS_COLLECT_SCANCOLS is enabled, check.
		if ((!this.skipAuthorization() && !qb.isInsideView() && HiveConf.getBoolVar(conf,
				HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED))
				|| HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
			qb.rewriteViewToSubq(alias, tabName, qbexpr, tab);
		} else {
			qb.rewriteViewToSubq(alias, tabName, qbexpr, null);
		}
	}

	private boolean skipAuthorization() {
		return true;
	}

	@SuppressWarnings("nls")
		// TODO: make aliases unique, otherwise needless rewriting takes place
	Integer genColListRegex(String colRegex, String tabAlias, ASTNode sel,
			ArrayList<ExprNodeDesc> colList, HashSet<ColumnInfo> excludeCols, HiveParserRowResolver input,
			HiveParserRowResolver colSrcRR, Integer pos, HiveParserRowResolver output, List<String> aliases,
			boolean ensureUniqueCols) throws SemanticException {
		if (colSrcRR == null) {
			colSrcRR = input;
		}
		// The table alias should exist
		if (tabAlias != null && !colSrcRR.hasTableAlias(tabAlias)) {
			throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));
		}

		// TODO: Have to put in the support for AS clause
		Pattern regex = null;
		try {
			regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel, e
					.getMessage()));
		}

		StringBuilder replacementText = new StringBuilder();
		int matched = 0;
		// add empty string to the list of aliases. Some operators (ex. GroupBy) add
		// ColumnInfos for table alias "".
		if (!aliases.contains("")) {
			aliases.add("");
		}
		/*
		 * track the input ColumnInfos that are added to the output.
		 * if a columnInfo has multiple mappings; then add the column only once,
		 * but carry the mappings forward.
		 */
		Map<ColumnInfo, ColumnInfo> inputColsProcessed = new HashMap<>();
		// For expr "*", aliases should be iterated in the order they are specified in the query.

		if (colSrcRR.getNamedJoinInfo() != null) {
			// We got using() clause in previous join. Need to generate select list as
			// per standard. For * we will have joining columns first non-repeated
			// followed by other columns.
			HashMap<String, ColumnInfo> leftMap = colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(0));
			HashMap<String, ColumnInfo> rightMap = colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(1));
			HashMap<String, ColumnInfo> chosenMap = null;
			if (colSrcRR.getNamedJoinInfo().getHiveJoinType() != JoinType.RIGHTOUTER) {
				chosenMap = leftMap;
			} else {
				chosenMap = rightMap;
			}
			// first get the columns in named columns
			for (String columnName : colSrcRR.getNamedJoinInfo().getNamedColumns()) {
				for (Map.Entry<String, ColumnInfo> entry : chosenMap.entrySet()) {
					ColumnInfo colInfo = entry.getValue();
					if (!columnName.equals(colInfo.getAlias())) {
						continue;
					}
					String name = colInfo.getInternalName();
					String[] tmp = colSrcRR.reverseLookup(name);

					// Skip the colinfos which are not for this particular alias
					if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
						continue;
					}

					if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
						continue;
					}
					ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
					if (oColInfo == null) {
						ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo.getType(), name,
								colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isSkewedCol());
						colList.add(expr);
						oColInfo = new ColumnInfo(getColumnInternalName(pos), colInfo.getType(),
								colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
						inputColsProcessed.put(colInfo, oColInfo);
					}
					if (ensureUniqueCols) {
						if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
							throw new CalciteSemanticException("Cannot add column to RR: " + tmp[0] + "."
									+ tmp[1] + " => " + oColInfo + " due to duplication, see previous warnings",
									UnsupportedFeature.Duplicates_in_RR);
						}
					} else {
						output.put(tmp[0], tmp[1], oColInfo);
					}
					pos = pos + 1;
					matched++;

					if (unparseTranslator.isEnabled()) {
						if (replacementText.length() > 0) {
							replacementText.append(", ");
						}
						replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
						replacementText.append(".");
						replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
					}
				}
			}
		}
		for (String alias : aliases) {
			HashMap<String, ColumnInfo> fMap = colSrcRR.getFieldMap(alias);
			if (fMap == null) {
				continue;
			}
			// For the tab.* case, add all the columns to the fieldList from the input schema
			for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
				ColumnInfo colInfo = entry.getValue();
				if (colSrcRR.getNamedJoinInfo() != null && colSrcRR.getNamedJoinInfo().getNamedColumns().contains(colInfo.getAlias())) {
					// we already added this column in select list.
					continue;
				}
				if (excludeCols != null && excludeCols.contains(colInfo)) {
					continue; // This was added during plan generation.
				}
				// First, look up the column from the source against which * is to be
				// resolved.
				// We'd later translated this into the column from proper input, if
				// it's valid.
				// TODO: excludeCols may be possible to remove using the same
				// technique.
				String name = colInfo.getInternalName();
				String[] tmp = colSrcRR.reverseLookup(name);

				// Skip the colinfos which are not for this particular alias
				if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
					continue;
				}

				if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
					continue;
				}

				// Not matching the regex?
				if (!regex.matcher(tmp[1]).matches()) {
					continue;
				}

				// If input (GBY) is different than the source of columns, find the
				// same column in input.
				// TODO: This is fraught with peril.
				if (input != colSrcRR) {
					colInfo = input.get(tabAlias, tmp[1]);
					if (colInfo == null) {
						LOG.error("Cannot find colInfo for " + tabAlias + "." + tmp[1] + ", derived from ["
								+ colSrcRR + "], in [" + input + "]");
						throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY, tmp[1]);
					}
					String oldCol = null;
					if (LOG.isDebugEnabled()) {
						oldCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
					}
					name = colInfo.getInternalName();
					tmp = input.reverseLookup(name);
					if (LOG.isDebugEnabled()) {
						String newCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
						LOG.debug("Translated [" + oldCol + "] to [" + newCol + "]");
					}
				}

				ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
				if (oColInfo == null) {
					ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo.getType(), name,
							colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isSkewedCol());
					colList.add(expr);
					oColInfo = new ColumnInfo(getColumnInternalName(pos), colInfo.getType(),
							colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
					inputColsProcessed.put(colInfo, oColInfo);
				}
				if (ensureUniqueCols) {
					if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
						throw new CalciteSemanticException("Cannot add column to RR: " + tmp[0] + "." + tmp[1]
								+ " => " + oColInfo + " due to duplication, see previous warnings",
								UnsupportedFeature.Duplicates_in_RR);
					}
				} else {
					output.put(tmp[0], tmp[1], oColInfo);
				}
				pos = pos.intValue() + 1;
				matched++;

				if (unparseTranslator.isEnabled()) {
					if (replacementText.length() > 0) {
						replacementText.append(", ");
					}
					replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
					replacementText.append(".");
					replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
				}
			}
		}

		if (matched == 0) {
			throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel));
		}

		if (unparseTranslator.isEnabled()) {
			unparseTranslator.addTranslation(sel, replacementText.toString());
		}
		return pos;
	}

	public static String getColumnInternalName(int pos) {
		return HiveConf.getColumnInternalName(pos);
	}

	protected List<Integer> getGroupingSetsForRollup(int size) {
		List<Integer> groupingSetKeys = new ArrayList<Integer>();
		for (int i = 0; i <= size; i++) {
			groupingSetKeys.add((1 << i) - 1);
		}
		return groupingSetKeys;
	}

	protected List<Integer> getGroupingSetsForCube(int size) {
		int count = 1 << size;
		List<Integer> results = new ArrayList<Integer>(count);
		for (int i = 0; i < count; ++i) {
			results.add(i);
		}
		return results;
	}

	protected List<Integer> getGroupingSets(List<ASTNode> groupByExpr, HiveParserQBParseInfo parseInfo,
			String dest) throws SemanticException {
		Map<String, Integer> exprPos = new HashMap<>();
		for (int i = 0; i < groupByExpr.size(); ++i) {
			ASTNode node = groupByExpr.get(i);
			exprPos.put(node.toStringTree(), i);
		}

		ASTNode root = parseInfo.getGroupByForClause(dest);
		List<Integer> result = new ArrayList<Integer>(root == null ? 0 : root.getChildCount());
		if (root != null) {
			for (int i = 0; i < root.getChildCount(); ++i) {
				ASTNode child = (ASTNode) root.getChild(i);
				if (child.getType() != HiveASTParser.TOK_GROUPING_SETS_EXPRESSION) {
					continue;
				}
				int bitmap = com.google.common.math.IntMath.pow(2, groupByExpr.size()) - 1;
				for (int j = 0; j < child.getChildCount(); ++j) {
					String treeAsString = child.getChild(j).toStringTree();
					Integer pos = exprPos.get(treeAsString);
					if (pos == null) {
						throw new SemanticException(
								HiveParserUtils.generateErrorMessage((ASTNode) child.getChild(j),
										ErrorMsg.HIVE_GROUPING_SETS_EXPR_NOT_IN_GROUPBY.getErrorCodedMsg()));
					}
					bitmap = HiveParserUtils.unsetBit(bitmap, groupByExpr.size() - pos - 1);
				}
				result.add(bitmap);
			}
		}
		if (checkForEmptyGroupingSets(result, com.google.common.math.IntMath.pow(2, groupByExpr.size()) - 1)) {
			throw new SemanticException("Empty grouping sets not allowed");
		}
		return result;
	}

	private boolean checkForEmptyGroupingSets(List<Integer> bitmaps, int groupingIdAllSet) {
		boolean ret = true;
		for (int mask : bitmaps) {
			ret &= mask == groupingIdAllSet;
		}
		return ret;
	}

	/**
	 * This function is a wrapper of parseInfo.getGroupByForClause which
	 * automatically translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY
	 * a,b,c.
	 */
	List<ASTNode> getGroupByForClause(HiveParserQBParseInfo parseInfo, String dest) throws SemanticException {
		if (parseInfo.getSelForClause(dest).getToken().getType() == HiveASTParser.TOK_SELECTDI) {
			ASTNode selectExprs = parseInfo.getSelForClause(dest);
			List<ASTNode> result = new ArrayList<>(selectExprs == null ? 0
					: selectExprs.getChildCount());
			if (selectExprs != null) {
				for (int i = 0; i < selectExprs.getChildCount(); ++i) {
					if (((ASTNode) selectExprs.getChild(i)).getToken().getType() == HiveASTParser.QUERY_HINT) {
						continue;
					}
					// table.column AS alias
					ASTNode grpbyExpr = (ASTNode) selectExprs.getChild(i).getChild(0);
					result.add(grpbyExpr);
				}
			}
			return result;
		} else {
			ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
			List<ASTNode> result = new ArrayList<>(grpByExprs == null ? 0
					: grpByExprs.getChildCount());
			if (grpByExprs != null) {
				for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
					ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
					if (grpbyExpr.getType() != HiveASTParser.TOK_GROUPING_SETS_EXPRESSION) {
						result.add(grpbyExpr);
					}
				}
			}
			return result;
		}
	}

	String recommendName(ExprNodeDesc exp, String colAlias) {
		if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
			return null;
		}
		String column = ExprNodeDescUtils.recommendInputName(exp);
		if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
			return column;
		}
		return null;
	}

	String getAutogenColAliasPrfxLbl() {
		return this.autogenColAliasPrfxLbl;
	}

	boolean autogenColAliasPrfxIncludeFuncName() {
		return this.autogenColAliasPrfxIncludeFuncName;
	}

	void checkExpressionsForGroupingSet(List<ASTNode> grpByExprs,
			List<ASTNode> distinctGrpByExprs,
			Map<String, ASTNode> aggregationTrees,
			HiveParserRowResolver inputRowResolver) throws SemanticException {

		Set<String> colNamesGroupByExprs = new HashSet<String>();
		Set<String> colNamesGroupByDistinctExprs = new HashSet<String>();
		Set<String> colNamesAggregateParameters = new HashSet<String>();

		// The columns in the group by expressions should not intersect with the columns in the
		// distinct expressions
		for (ASTNode grpByExpr : grpByExprs) {
			HiveParserUtils.extractColumns(colNamesGroupByExprs, genExprNodeDesc(grpByExpr, inputRowResolver));
		}

		// If there is a distinctFuncExp, add all parameters to the reduceKeys.
		if (!distinctGrpByExprs.isEmpty()) {
			for (ASTNode value : distinctGrpByExprs) {
				// 0 is function name
				for (int i = 1; i < value.getChildCount(); i++) {
					ASTNode parameter = (ASTNode) value.getChild(i);
					ExprNodeDesc distExprNode = genExprNodeDesc(parameter, inputRowResolver);
					// extract all the columns
					HiveParserUtils.extractColumns(colNamesGroupByDistinctExprs, distExprNode);
				}

				if (HiveParserUtils.hasCommonElement(colNamesGroupByExprs, colNamesGroupByDistinctExprs)) {
					throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
				}
			}
		}

		for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
			ASTNode value = entry.getValue();
			ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
			// 0 is the function name
			for (int i = 1; i < value.getChildCount(); i++) {
				ASTNode paraExpr = (ASTNode) value.getChild(i);
				ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRowResolver);

				// extract all the columns
				HiveParserUtils.extractColumns(colNamesAggregateParameters, paraExprNode);
			}

			if (HiveParserUtils.hasCommonElement(colNamesGroupByExprs, colNamesAggregateParameters)) {
				throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
			}
		}
	}

	protected String getAliasId(String alias, HiveParserQB qb) {
		return (qb.getId() == null ? alias : qb.getId() + ":" + alias).toLowerCase();
	}

	@SuppressWarnings("nls")
	public Phase1Ctx initPhase1Ctx() {

		Phase1Ctx ctx1 = new Phase1Ctx();
		ctx1.nextNum = 0;
		ctx1.dest = "reduce";

		return ctx1;
	}

	public void init(boolean clearPartsCache) {
		// clear most members
		reset(clearPartsCache);

		// init
		HiveParserQB qb = new HiveParserQB(null, null, false);
		this.qb = qb;
	}

	private Table getTableObjectByName(String tableName) throws HiveException {
		if (!tabNameToTabObject.containsKey(tableName)) {
			Table table = db.getTable(tableName);
			tabNameToTabObject.put(tableName, table);
			return table;
		} else {
			return tabNameToTabObject.get(tableName);
		}
	}

	boolean genResolvedParseTree(ASTNode ast, HiveParserPlannerContext plannerCtx) throws SemanticException {
		ASTNode child = ast;
		this.ast = ast;
		viewsExpanded = new ArrayList<>();
		ctesExpanded = new ArrayList<>();

		// 1. analyze and process the position alias
		// step processPositionAlias out of genResolvedParseTree

		// 2. analyze create table command
		// create table won't get here
		queryState.setCommandType(HiveOperation.QUERY);

		// 3. analyze create view command
		// create view won't get here

		// 4. continue analyzing from the child ASTNode.
		Phase1Ctx ctx1 = initPhase1Ctx();
		preProcessForInsert(child, qb);
		if (!doPhase1(child, qb, ctx1, plannerCtx)) {
			// if phase1Result false return
			return false;
		}
		LOG.info("Completed phase 1 of Semantic Analysis");

		// 5. Resolve Parse Tree
		// Materialization is allowed if it is not a view definition
		getMetaData(qb, createVwDesc == null);
		LOG.info("Completed getting MetaData in Semantic Analysis");
		plannerCtx.setParseTreeAttr(child, ctx1);
		return true;
	}

	/**
	 * This will walk AST of an INSERT statement and assemble a list of target tables
	 * which are in an HDFS encryption zone.  This is needed to make sure that so that
	 * the data from values clause of Insert ... select values(...) is stored securely.
	 * See also {@link #genValuesTempTable(ASTNode, HiveParserQB)}
	 */
	private void preProcessForInsert(ASTNode node, HiveParserQB qb) throws SemanticException {
		try {
			if (!(node != null && node.getToken() != null && node.getToken().getType() == HiveASTParser.TOK_QUERY)) {
				return;
			}
			for (Node child : node.getChildren()) {
				//each insert of multi insert looks like
				//(TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME T1)))
				if (((ASTNode) child).getToken().getType() != HiveASTParser.TOK_INSERT) {
					continue;
				}
				ASTNode n = (ASTNode) ((ASTNode) child).getFirstChildWithType(HiveASTParser.TOK_INSERT_INTO);
				if (n == null) {
					continue;
				}
				n = (ASTNode) n.getFirstChildWithType(HiveASTParser.TOK_TAB);
				if (n == null) {
					continue;
				}
				n = (ASTNode) n.getFirstChildWithType(HiveASTParser.TOK_TABNAME);
				if (n == null) {
					continue;
				}
				String[] dbTab = getQualifiedTableName(n);
				db.getTable(dbTab[0], dbTab[1]);
			}
		} catch (Exception ex) {
			throw new SemanticException(ex);
		}
	}

	// Generates an expression node descriptor for the expression with HiveParserTypeCheckCtx.
	public ExprNodeDesc genExprNodeDesc(ASTNode expr, HiveParserRowResolver input)
			throws SemanticException {
		// Since the user didn't supply a customized type-checking context,
		// use default settings.
		return genExprNodeDesc(expr, input, true, false);
	}

	public ExprNodeDesc genExprNodeDesc(ASTNode expr, HiveParserRowResolver input,
			HiveParserRowResolver outerRR, Map<ASTNode, RelNode> subqueryToRelNode,
			boolean useCaching) throws SemanticException {

		HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(input, useCaching, false);
		tcCtx.setOuterRR(outerRR);
		tcCtx.setSubqueryToRelNode(subqueryToRelNode);
		return genExprNodeDesc(expr, input, tcCtx);
	}

	public ExprNodeDesc genExprNodeDesc(ASTNode expr, HiveParserRowResolver input, boolean useCaching,
			boolean foldExpr) throws SemanticException {
		HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(input, useCaching, foldExpr);
		return genExprNodeDesc(expr, input, tcCtx);
	}

	/**
	 * Generates an expression node descriptors for the expression and children of it
	 * with default HiveParserTypeCheckCtx.
	 */
	public Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, HiveParserRowResolver input)
			throws SemanticException {
		HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(input);
		return genAllExprNodeDesc(expr, input, tcCtx);
	}

	/**
	 * Returns expression node descriptor for the expression.
	 * If it's evaluated already in previous operator, it can be retrieved from cache.
	 */
	public ExprNodeDesc genExprNodeDesc(ASTNode expr, HiveParserRowResolver input,
			HiveParserTypeCheckCtx tcCtx) throws SemanticException {
		// We recursively create the exprNodeDesc. Base cases: when we encounter
		// a column ref, we convert that into an exprNodeColumnDesc; when we
		// encounter
		// a constant, we convert that into an exprNodeConstantDesc. For others we
		// just
		// build the exprNodeFuncDesc with recursively built children.

		// If the current subExpression is pre-calculated, as in Group-By etc.
		ExprNodeDesc cached = null;
		if (tcCtx.isUseCaching()) {
			cached = getExprNodeDescCached(expr, input);
		}
		if (cached == null) {
			Map<ASTNode, ExprNodeDesc> allExprs = genAllExprNodeDesc(expr, input, tcCtx);
			return allExprs.get(expr);
		}
		return cached;
	}

	// Find ExprNodeDesc for the expression cached in the HiveParserRowResolver. Returns null if not exists.
	private ExprNodeDesc getExprNodeDescCached(ASTNode expr, HiveParserRowResolver input)
			throws SemanticException {
		ColumnInfo colInfo = input.getExpression(expr);
		if (colInfo != null) {
			ASTNode source = input.getExpressionSource(expr);
			if (source != null) {
				unparseTranslator.addCopyTranslation(expr, source);
			}
			return new ExprNodeColumnDesc(colInfo.getType(), colInfo
					.getInternalName(), colInfo.getTabAlias(), colInfo
					.getIsVirtualCol(), colInfo.isSkewedCol());
		}
		return null;
	}

	/**
	 * Generates all of the expression node descriptors for the expression and children of it
	 * passed in the arguments. This function uses the row resolver and the metadata information
	 * that are passed as arguments to resolve the column names to internal names.
	 */
	@SuppressWarnings("nls")
	public Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, HiveParserRowResolver input,
			HiveParserTypeCheckCtx tcCtx) throws SemanticException {
		// Create the walker and  the rules dispatcher.
		tcCtx.setUnparseTranslator(unparseTranslator);

		Map<ASTNode, ExprNodeDesc> nodeOutputs =
				HiveParserTypeCheckProcFactory.genExprNode(expr, tcCtx);
		ExprNodeDesc desc = nodeOutputs.get(expr);
		if (desc == null) {
			String errMsg = tcCtx.getError();
			if (errMsg == null) {
				errMsg = "Error in parsing ";
			}
			throw new SemanticException(errMsg);
		}
		if (desc instanceof ExprNodeColumnListDesc) {
			throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
		}

		if (!unparseTranslator.isEnabled()) {
			// Not creating a view, so no need to track view expansions.
			return nodeOutputs;
		}

		Map<ExprNodeDesc, String> nodeToText = new HashMap<>();
		List<ASTNode> fieldDescList = new ArrayList<>();

		for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
			if (!(entry.getValue() instanceof ExprNodeColumnDesc)) {
				// we need to translate the ExprNodeFieldDesc too, e.g., identifiers in
				// struct<>.
				if (entry.getValue() instanceof ExprNodeFieldDesc) {
					fieldDescList.add(entry.getKey());
				}
				continue;
			}
			ASTNode node = entry.getKey();
			ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) entry.getValue();
			if ((columnDesc.getTabAlias() == null)
					|| (columnDesc.getTabAlias().length() == 0)) {
				// These aren't real column refs; instead, they are special
				// internal expressions used in the representation of aggregation.
				continue;
			}
			String[] tmp = input.reverseLookup(columnDesc.getColumn());
			// in subquery case, tmp may be from outside.
			if (tmp[0] != null && columnDesc.getTabAlias() != null
					&& !tmp[0].equals(columnDesc.getTabAlias()) && tcCtx.getOuterRR() != null) {
				tmp = tcCtx.getOuterRR().reverseLookup(columnDesc.getColumn());
			}
			StringBuilder replacementText = new StringBuilder();
			replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
			replacementText.append(".");
			replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
			nodeToText.put(columnDesc, replacementText.toString());
			unparseTranslator.addTranslation(node, replacementText.toString());
		}

		for (ASTNode node : fieldDescList) {
			Map<ASTNode, String> map = translateFieldDesc(node);
			for (Entry<ASTNode, String> entry : map.entrySet()) {
				unparseTranslator.addTranslation(entry.getKey(), entry.getValue());
			}
		}

		return nodeOutputs;
	}

	private Map<ASTNode, String> translateFieldDesc(ASTNode node) {
		Map<ASTNode, String> map = new HashMap<>();
		if (node.getType() == HiveASTParser.DOT) {
			for (Node child : node.getChildren()) {
				map.putAll(translateFieldDesc((ASTNode) child));
			}
		} else if (node.getType() == HiveASTParser.Identifier) {
			map.put(node, HiveUtils.unparseIdentifier(node.getText(), conf));
		}
		return map;
	}

	// Process the position alias in GROUPBY and ORDERBY
	public void processPositionAlias(ASTNode ast) throws SemanticException {
		boolean isBothByPos = HiveConf.getBoolVar(conf, ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS);
		boolean isGbyByPos = isBothByPos || Boolean.parseBoolean(conf.get("hive.groupby.position.alias", "false"));
		boolean isObyByPos = isBothByPos || Boolean.parseBoolean(conf.get("hive.orderby.position.alias", "true"));

		Deque<ASTNode> stack = new ArrayDeque<>();
		stack.push(ast);

		while (!stack.isEmpty()) {
			ASTNode next = stack.pop();

			if (next.getChildCount() == 0) {
				continue;
			}

			boolean isAllCol;
			ASTNode selectNode = null;
			ASTNode groupbyNode = null;
			ASTNode orderbyNode = null;

			// get node type
			int childCount = next.getChildCount();
			for (int childPos = 0; childPos < childCount; ++childPos) {
				ASTNode node = (ASTNode) next.getChild(childPos);
				int type = node.getToken().getType();
				if (type == HiveASTParser.TOK_SELECT) {
					selectNode = node;
				} else if (type == HiveASTParser.TOK_GROUPBY) {
					groupbyNode = node;
				} else if (type == HiveASTParser.TOK_ORDERBY) {
					orderbyNode = node;
				}
			}

			if (selectNode != null) {
				int selectExpCnt = selectNode.getChildCount();

				// replace each of the position alias in GROUPBY with the actual column name
				if (groupbyNode != null) {
					for (int childPos = 0; childPos < groupbyNode.getChildCount(); ++childPos) {
						ASTNode node = (ASTNode) groupbyNode.getChild(childPos);
						if (node.getToken().getType() == HiveASTParser.Number) {
							if (isGbyByPos) {
								int pos = Integer.parseInt(node.getText());
								if (pos > 0 && pos <= selectExpCnt) {
									groupbyNode.setChild(childPos,
											selectNode.getChild(pos - 1).getChild(0));
								} else {
									throw new SemanticException(
											ErrorMsg.INVALID_POSITION_ALIAS_IN_GROUPBY.getMsg(
													"Position alias: " + pos + " does not exist\n" +
															"The Select List is indexed from 1 to " + selectExpCnt));
								}
							} else {
								warn("Using constant number  " + node.getText() +
										" in group by. If you try to use position alias when hive.groupby.position.alias is false, the position alias will be ignored.");
							}
						}
					}
				}

				// replace each of the position alias in ORDERBY with the actual column name
				if (orderbyNode != null) {
					isAllCol = false;
					for (int childPos = 0; childPos < selectNode.getChildCount(); ++childPos) {
						ASTNode node = (ASTNode) selectNode.getChild(childPos).getChild(0);
						if (node != null && node.getToken().getType() == HiveASTParser.TOK_ALLCOLREF) {
							isAllCol = true;
						}
					}
					for (int childPos = 0; childPos < orderbyNode.getChildCount(); ++childPos) {
						ASTNode colNode = (ASTNode) orderbyNode.getChild(childPos).getChild(0);
						ASTNode node = (ASTNode) colNode.getChild(0);
						if (node != null && node.getToken().getType() == HiveASTParser.Number) {
							if (isObyByPos) {
								if (!isAllCol) {
									int pos = Integer.parseInt(node.getText());
									if (pos > 0 && pos <= selectExpCnt) {
										colNode.setChild(0, selectNode.getChild(pos - 1).getChild(0));
									} else {
										throw new SemanticException(
												ErrorMsg.INVALID_POSITION_ALIAS_IN_ORDERBY.getMsg(
														"Position alias: " + pos + " does not exist\n" +
																"The Select List is indexed from 1 to " + selectExpCnt));
									}
								} else {
									throw new SemanticException(
											ErrorMsg.NO_SUPPORTED_ORDERBY_ALLCOLREF_POS.getMsg());
								}
							} else { //if not using position alias and it is a number.
								warn("Using constant number " + node.getText() +
										" in order by. If you try to use position alias when hive.orderby.position.alias is false, the position alias will be ignored.");
							}
						}
					}
				}
			}

			for (int i = next.getChildren().size() - 1; i >= 0; i--) {
				stack.push((ASTNode) next.getChildren().get(i));
			}
		}
	}

	public HiveParserQB getQB() {
		return qb;
	}

	public void setQB(HiveParserQB qb) {
		this.qb = qb;
	}

//--------------------------- PTF handling -----------------------------------

	/*
	 * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
	 *   PTF invocation.
	 * - For a TABLEREF: set the source to the alias returned by processTable
	 * - For a SubQuery: set the source to the alias returned by processSubQuery
	 * - For a PTF invocation: recursively call processPTFChain.
	 */
	private PTFInputSpec processPTFSource(HiveParserQB qb, ASTNode inputNode) throws SemanticException {

		PTFInputSpec qInSpec = null;
		int type = inputNode.getType();
		String alias;
		switch (type) {
			case HiveASTParser.TOK_TABREF:
				alias = processTable(qb, inputNode);
				qInSpec = new PTFQueryInputSpec();
				((PTFQueryInputSpec) qInSpec).setType(PTFQueryInputType.TABLE);
				((PTFQueryInputSpec) qInSpec).setSource(alias);
				break;
			case HiveASTParser.TOK_SUBQUERY:
				alias = processSubQuery(qb, inputNode);
				qInSpec = new PTFQueryInputSpec();
				((PTFQueryInputSpec) qInSpec).setType(PTFQueryInputType.SUBQUERY);
				((PTFQueryInputSpec) qInSpec).setSource(alias);
				break;
			case HiveASTParser.TOK_PTBLFUNCTION:
				qInSpec = processPTFChain(qb, inputNode);
				break;
			default:
				throw new SemanticException(HiveParserUtils.generateErrorMessage(inputNode,
						"Unknown input type to PTF"));
		}

		qInSpec.setAstNode(inputNode);
		return qInSpec;

	}

	/*
	 * - tree form is
	 *   ^(TOK_PTBLFUNCTION name alias? partitionTableFunctionSource partitioningSpec? arguments*)
	 * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
	 *   PTF invocation.
	 */
	private PartitionedTableFunctionSpec processPTFChain(HiveParserQB qb, ASTNode ptf)
			throws SemanticException {
		int childCount = ptf.getChildCount();
		if (childCount < 2) {
			throw new SemanticException(HiveParserUtils.generateErrorMessage(ptf,
					"Not enough Children " + childCount));
		}

		PartitionedTableFunctionSpec ptfSpec = new PartitionedTableFunctionSpec();
		ptfSpec.setAstNode(ptf);

		// name
		ASTNode nameNode = (ASTNode) ptf.getChild(0);
		ptfSpec.setName(nameNode.getText());

		int inputIdx = 1;

		// alias
		ASTNode secondChild = (ASTNode) ptf.getChild(1);
		if (secondChild.getType() == HiveASTParser.Identifier) {
			ptfSpec.setAlias(secondChild.getText());
			inputIdx++;
		}

		// input
		ASTNode inputNode = (ASTNode) ptf.getChild(inputIdx);
		ptfSpec.setInput(processPTFSource(qb, inputNode));

		int argStartIdx = inputIdx + 1;

		// partitioning Spec
		int pSpecIdx = inputIdx + 1;
		ASTNode pSpecNode = ptf.getChildCount() > inputIdx ?
				(ASTNode) ptf.getChild(pSpecIdx) : null;
		if (pSpecNode != null && pSpecNode.getType() == HiveASTParser.TOK_PARTITIONINGSPEC) {
			PartitioningSpec partitioning = processPTFPartitionSpec(pSpecNode);
			ptfSpec.setPartitioning(partitioning);
			argStartIdx++;
		}

		// arguments
		for (int i = argStartIdx; i < ptf.getChildCount(); i++) {
			ptfSpec.addArg((ASTNode) ptf.getChild(i));
		}
		return ptfSpec;
	}

	/*
	 * - invoked during FROM AST tree processing, on encountering a PTF invocation.
	 * - tree form is
	 *   ^(TOK_PTBLFUNCTION name partitionTableFunctionSource partitioningSpec? arguments*)
	 * - setup a HiveParserPTFInvocationSpec for this top level PTF invocation.
	 */
	private void processPTF(HiveParserQB qb, ASTNode ptf) throws SemanticException {

		PartitionedTableFunctionSpec ptfSpec = processPTFChain(qb, ptf);

		if (ptfSpec.getAlias() != null) {
			qb.addAlias(ptfSpec.getAlias());
		}

		HiveParserPTFInvocationSpec spec = new HiveParserPTFInvocationSpec();
		spec.setFunction(ptfSpec);
		qb.addPTFNodeToSpec(ptf, spec);
	}

	private void handleQueryWindowClauses(HiveParserQB qb, Phase1Ctx ctx1, ASTNode node)
			throws SemanticException {
		HiveParserWindowingSpec spec = qb.getWindowingSpec(ctx1.dest);
		for (int i = 0; i < node.getChildCount(); i++) {
			processQueryWindowClause(spec, (ASTNode) node.getChild(i));
		}
	}

	private PartitionSpec processPartitionSpec(ASTNode node) {
		PartitionSpec pSpec = new PartitionSpec();
		int exprCnt = node.getChildCount();
		for (int i = 0; i < exprCnt; i++) {
			PartitionExpression exprSpec = new PartitionExpression();
			exprSpec.setExpression((ASTNode) node.getChild(i));
			pSpec.addExpression(exprSpec);
		}
		return pSpec;
	}

	private OrderSpec processOrderSpec(ASTNode sortNode) {
		OrderSpec oSpec = new OrderSpec();
		int exprCnt = sortNode.getChildCount();
		for (int i = 0; i < exprCnt; i++) {
			OrderExpression exprSpec = new OrderExpression();
			ASTNode orderSpec = (ASTNode) sortNode.getChild(i);
			ASTNode nullOrderSpec = (ASTNode) orderSpec.getChild(0);
			exprSpec.setExpression((ASTNode) nullOrderSpec.getChild(0));
			if (orderSpec.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
				exprSpec.setOrder(Order.ASC);
			} else {
				exprSpec.setOrder(Order.DESC);
			}
			if (nullOrderSpec.getType() == HiveASTParser.TOK_NULLS_FIRST) {
				exprSpec.setNullOrder(HiveParserPTFInvocationSpec.NullOrder.NULLS_FIRST);
			} else {
				exprSpec.setNullOrder(HiveParserPTFInvocationSpec.NullOrder.NULLS_LAST);
			}
			oSpec.addExpression(exprSpec);
		}
		return oSpec;
	}

	private PartitioningSpec processPTFPartitionSpec(ASTNode pSpecNode) {
		PartitioningSpec partitioning = new PartitioningSpec();
		ASTNode firstChild = (ASTNode) pSpecNode.getChild(0);
		int type = firstChild.getType();
		int exprCnt;

		if (type == HiveASTParser.TOK_DISTRIBUTEBY || type == HiveASTParser.TOK_CLUSTERBY) {
			PartitionSpec pSpec = processPartitionSpec(firstChild);
			partitioning.setPartSpec(pSpec);
			ASTNode sortNode = pSpecNode.getChildCount() > 1 ? (ASTNode) pSpecNode.getChild(1) : null;
			if (sortNode != null) {
				OrderSpec oSpec = processOrderSpec(sortNode);
				partitioning.setOrderSpec(oSpec);
			}
		} else if (type == HiveASTParser.TOK_SORTBY || type == HiveASTParser.TOK_ORDERBY) {
			ASTNode sortNode = firstChild;
			OrderSpec oSpec = processOrderSpec(sortNode);
			partitioning.setOrderSpec(oSpec);
		}
		return partitioning;
	}

	private WindowFunctionSpec processWindowFunction(ASTNode node, ASTNode wsNode)
			throws SemanticException {
		WindowFunctionSpec wfSpec = new WindowFunctionSpec();

		switch (node.getType()) {
			case HiveASTParser.TOK_FUNCTIONSTAR:
				wfSpec.setStar(true);
				break;
			case HiveASTParser.TOK_FUNCTIONDI:
				wfSpec.setDistinct(true);
				break;
		}

		wfSpec.setExpression(node);

		ASTNode nameNode = (ASTNode) node.getChild(0);
		wfSpec.setName(nameNode.getText());

		for (int i = 1; i < node.getChildCount() - 1; i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			wfSpec.addArg(child);
		}

		if (wsNode != null) {
			WindowSpec ws = processWindowSpec(wsNode);
			wfSpec.setWindowSpec(ws);
		}

		return wfSpec;
	}

	private boolean containsLeadLagUDF(ASTNode expressionTree) {
		int exprTokenType = expressionTree.getToken().getType();
		if (exprTokenType == HiveASTParser.TOK_FUNCTION) {
			assert (expressionTree.getChildCount() != 0);
			if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
				String functionName = unescapeIdentifier(expressionTree.getChild(0)
						.getText());
				functionName = functionName.toLowerCase();
				if (FunctionRegistry.LAG_FUNC_NAME.equals(functionName) ||
						FunctionRegistry.LEAD_FUNC_NAME.equals(functionName)
				) {
					return true;
				}
			}
		}
		for (int i = 0; i < expressionTree.getChildCount(); i++) {
			if (containsLeadLagUDF((ASTNode) expressionTree.getChild(i))) {
				return true;
			}
		}
		return false;
	}

	private void processQueryWindowClause(HiveParserWindowingSpec spec, ASTNode node)
			throws SemanticException {
		ASTNode nameNode = (ASTNode) node.getChild(0);
		ASTNode wsNode = (ASTNode) node.getChild(1);
		if (spec.getWindowSpecs() != null && spec.getWindowSpecs().containsKey(nameNode.getText())) {
			throw new SemanticException(HiveParserUtils.generateErrorMessage(nameNode,
					"Duplicate definition of window " + nameNode.getText() +
							" is not allowed"));
		}
		WindowSpec ws = processWindowSpec(wsNode);
		spec.addWindowSpec(nameNode.getText(), ws);
	}

	private WindowSpec processWindowSpec(ASTNode node) throws SemanticException {
		String sourceId = null;
		PartitionSpec partition = null;
		OrderSpec order = null;
		WindowFrameSpec windowFrame = null;

		boolean hasSrcId = false, hasPartSpec = false, hasWF = false;
		int srcIdIdx = -1, partIdx = -1, wfIdx = -1;

		for (int i = 0; i < node.getChildCount(); i++) {
			int type = node.getChild(i).getType();
			switch (type) {
				case HiveASTParser.Identifier:
					hasSrcId = true;
					srcIdIdx = i;
					break;
				case HiveASTParser.TOK_PARTITIONINGSPEC:
					hasPartSpec = true;
					partIdx = i;
					break;
				case HiveASTParser.TOK_WINDOWRANGE:
				case HiveASTParser.TOK_WINDOWVALUES:
					hasWF = true;
					wfIdx = i;
					break;
			}
		}

		WindowSpec ws = new WindowSpec();

		if (hasSrcId) {
			ASTNode nameNode = (ASTNode) node.getChild(srcIdIdx);
			ws.setSourceId(nameNode.getText());
		}

		if (hasPartSpec) {
			ASTNode partNode = (ASTNode) node.getChild(partIdx);
			PartitioningSpec partitioning = processPTFPartitionSpec(partNode);
			ws.setPartitioning(partitioning);
		}

		if (hasWF) {
			ASTNode wfNode = (ASTNode) node.getChild(wfIdx);
			WindowFrameSpec wfSpec = processWindowFrame(wfNode);
			ws.setWindowFrame(wfSpec);
		}
		return ws;
	}

	private WindowFrameSpec processWindowFrame(ASTNode node) throws SemanticException {
		int type = node.getType();
		BoundarySpec start = null, end = null;
		/*
		 * A WindowFrame may contain just the Start Boundary or in the
		 * between style of expressing a WindowFrame both boundaries
		 * are specified.
		 */
		start = processBoundary(type, (ASTNode) node.getChild(0));
		if (node.getChildCount() > 1) {
			end = processBoundary(type, (ASTNode) node.getChild(1));
		}
		// Note: TOK_WINDOWVALUES means RANGE type, TOK_WINDOWRANGE means ROWS type
		return new WindowFrameSpec(type == HiveASTParser.TOK_WINDOWVALUES ? WindowType.RANGE : WindowType.ROWS, start, end);
	}

	private BoundarySpec processBoundary(int frameType, ASTNode node) throws SemanticException {
		BoundarySpec bs = new BoundarySpec();
		int type = node.getType();
		boolean hasAmt = true;

		switch (type) {
			case HiveASTParser.KW_PRECEDING:
				bs.setDirection(Direction.PRECEDING);
				break;
			case HiveASTParser.KW_FOLLOWING:
				bs.setDirection(Direction.FOLLOWING);
				break;
			case HiveASTParser.KW_CURRENT:
				bs.setDirection(Direction.CURRENT);
				hasAmt = false;
				break;
		}

		if (hasAmt) {
			ASTNode amtNode = (ASTNode) node.getChild(0);
			if (amtNode.getType() == HiveASTParser.KW_UNBOUNDED) {
				bs.setAmt(BoundarySpec.UNBOUNDED_AMOUNT);
			} else {
				int amt = Integer.parseInt(amtNode.getText());
				if (amt <= 0) {
					throw new SemanticException(
							"Window Frame Boundary Amount must be a positive integer, provided amount is: " + amt);
				}
				bs.setAmt(amt);
			}
		}
		return bs;
	}

	private void warn(String msg) {
		SessionState.getConsole().printInfo(
				String.format("Warning: %s", msg));
	}

	class CTEClause {
		CTEClause(String alias, ASTNode cteNode) {
			this.alias = alias;
			this.cteNode = cteNode;
		}

		String alias;
		ASTNode cteNode;
		boolean materialize;
		int reference;
		HiveParserQBExpr qbExpr;
		List<CTEClause> parents = new ArrayList<>();

		@Override
		public String toString() {
			return alias == null ? "<root>" : alias;
		}
	}
}
