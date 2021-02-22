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
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.hive.HiveGenericUDAF;
import org.apache.flink.table.functions.hive.HiveGenericUDTF;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.hive.optimizer.calcite.translator.HiveParserJoinCondTypeCheckProcFactory;
import org.apache.flink.table.planner.delegation.hive.optimizer.calcite.translator.HiveParserTypeConverter;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParseDriver;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserRowResolver;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserSemanticAnalyzer.GenericUDAFInfo;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserTypeCheckCtx;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserTypeCheckProcFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.HiveAggSqlFunction;
import org.apache.flink.table.planner.functions.utils.HiveTableSqlFunction;
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.delegation.hive.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.flink.table.planner.delegation.hive.parse.HiveParserSemanticAnalyzer.getColumnInternalName;
import static org.apache.flink.table.planner.delegation.hive.parse.HiveParserTypeCheckProcFactory.DefaultExprProcessor.getFunctionText;

/**
 * Util class for the hive planner.
 */
public class HiveParserUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParserUtils.class);

	private static final Class immutableListClz = HiveReflectionUtils.tryGetClass("com.google.common.collect.ImmutableList");
	private static final Class shadedImmutableListClz = HiveReflectionUtils.tryGetClass(
			"org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList");
	private static final boolean useShadedImmutableList = shadedImmutableListClz != null;

	private HiveParserUtils() {
	}

	public static void removeASTChild(ASTNode node) {
		Tree parent = node.getParent();
		if (parent != null) {
			parent.deleteChild(node.getChildIndex());
			node.setParent(null);
		}
	}

	public static NlsString asUnicodeString(String text) {
		return new NlsString(text, ConversionUtil.NATIVE_UTF16_CHARSET_NAME, SqlCollation.IMPLICIT);
	}

	// Overrides CalcitePlanner::canHandleQbForCbo to support SORT BY, CLUSTER BY, etc.
	public static String canHandleQbForCbo(QueryProperties queryProperties) {
		if (!queryProperties.hasPTF() && !queryProperties.usesScript()) {
			return null;
		}
		String msg = "";
		if (queryProperties.hasPTF()) {
			msg += "has PTF; ";
		}
		if (queryProperties.usesScript()) {
			msg += "uses scripts; ";
		}
		return msg;
	}

	// converts a hive TypeInfo to RelDataType
	public static RelDataType toRelDataType(TypeInfo typeInfo, RelDataTypeFactory relTypeFactory)
			throws SemanticException {
		RelDataType res;
		switch (typeInfo.getCategory()) {
			case PRIMITIVE:
				// hive sets NULLABLE for all primitive types, revert that
				res = HiveParserTypeConverter.convert(typeInfo, relTypeFactory);
				// TODO: do we need this?
//					return res;
				return relTypeFactory.createTypeWithNullability(res, false);
			case LIST:
				RelDataType elementType = toRelDataType(((ListTypeInfo) typeInfo).getListElementTypeInfo(), relTypeFactory);
				return relTypeFactory.createArrayType(elementType, -1);
			case MAP:
				RelDataType keyType = toRelDataType(((MapTypeInfo) typeInfo).getMapKeyTypeInfo(), relTypeFactory);
				RelDataType valType = toRelDataType(((MapTypeInfo) typeInfo).getMapValueTypeInfo(), relTypeFactory);
				return relTypeFactory.createMapType(keyType, valType);
			case STRUCT:
				List<TypeInfo> types = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
				List<RelDataType> convertedTypes = new ArrayList<>(types.size());
				for (TypeInfo type : types) {
					convertedTypes.add(toRelDataType(type, relTypeFactory));
				}
				return relTypeFactory.createStructType(convertedTypes, ((StructTypeInfo) typeInfo).getAllStructFieldNames());
			case UNION:
			default:
				throw new SemanticException(String.format("%s type is not supported yet", typeInfo.getCategory().name()));
		}
	}

	/**
	 * Proxy to Proxy to
	 * {@link RexBuilder#makeOver(RelDataType, SqlAggFunction, List, List, com.google.common.collect.ImmutableList,
	 * RexWindowBound, RexWindowBound, boolean, boolean, boolean, boolean, boolean)}.
	 */
	public static RexNode makeOver(RexBuilder rexBuilder, RelDataType type, SqlAggFunction operator, List<RexNode> exprs, List<RexNode> partitionKeys,
			List<RexFieldCollation> orderKeys, RexWindowBound lowerBound, RexWindowBound upperBound,
			boolean physical, boolean allowPartial, boolean nullWhenCountZero, boolean distinct, boolean ignoreNulls) {
		Preconditions.checkState(immutableListClz != null || shadedImmutableListClz != null,
				"Neither original nor shaded guava class can be found");
		Method method = null;
		final String methodName = "makeOver";
		final int orderKeysIndex = 4;
		Class[] argTypes = new Class[]{RelDataType.class, SqlAggFunction.class, List.class,
				List.class, null, RexWindowBound.class, RexWindowBound.class,
				boolean.class, boolean.class, boolean.class, boolean.class, boolean.class};
		if (immutableListClz != null) {
			argTypes[orderKeysIndex] = immutableListClz;
			method = HiveReflectionUtils.tryGetMethod(rexBuilder.getClass(), methodName, argTypes);
		}
		if (method == null) {
			Preconditions.checkState(shadedImmutableListClz != null,
					String.format("Shaded guava class not found, but method %s takes shaded parameter", methodName));
			argTypes[orderKeysIndex] = shadedImmutableListClz;
			method = HiveReflectionUtils.tryGetMethod(rexBuilder.getClass(), methodName, argTypes);
		}
		Preconditions.checkState(method != null, "Neither original nor shaded method can be found");
		Object orderKeysArg = toImmutableList(orderKeys);

		Object[] args = new Object[]{type, operator, exprs, partitionKeys, orderKeysArg, lowerBound, upperBound,
				physical, allowPartial, nullWhenCountZero, distinct, ignoreNulls};
		try {
			return (RexNode) method.invoke(rexBuilder, args);
		} catch (InvocationTargetException | IllegalAccessException e) {
			throw new RuntimeException("Failed to invoke " + methodName, e);
		}
	}

	// converts a collection to guava ImmutableList
	private static Object toImmutableList(Collection collection) {
		try {
			Class clz = useShadedImmutableList ? shadedImmutableListClz : immutableListClz;
			return HiveReflectionUtils.invokeMethod(clz, null, "copyOf", new Class[]{Collection.class}, new Object[]{collection});
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to create immutable list", e);
		}
	}

	// creates LogicalValues node
	public static RelNode genValuesRelNode(RelOptCluster cluster, RelDataType rowType, List<List<RexLiteral>> rows) {
		List<Object> immutableRows = rows.stream().map(HiveParserUtils::toImmutableList)
				.collect(Collectors.toList());
		Class[] argTypes = new Class[]{RelOptCluster.class, RelDataType.class, null};
		if (useShadedImmutableList) {
			argTypes[2] = HiveParserUtils.shadedImmutableListClz;
		} else {
			argTypes[2] = HiveParserUtils.immutableListClz;
		}
		Method method = HiveReflectionUtils.tryGetMethod(LogicalValues.class, "create", argTypes);
		Preconditions.checkState(method != null, "Cannot get the method to create LogicalValues");
		try {
			return (RelNode) method.invoke(null, cluster, rowType, HiveParserUtils.toImmutableList(immutableRows));
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to create LogicalValues", e);
		}
	}

	/**
	 * Proxy to {@link RexSubQuery#in(RelNode, com.google.common.collect.ImmutableList)}.
	 */
	public static RexSubQuery rexSubQueryIn(RelNode relNode, Collection<RexNode> rexNodes) {
		Class[] argTypes = new Class[]{RelNode.class, null};
		argTypes[1] = useShadedImmutableList ? shadedImmutableListClz : immutableListClz;
		Method method = HiveReflectionUtils.tryGetMethod(RexSubQuery.class, "in", argTypes);
		Preconditions.checkState(method != null, "Cannot get the method to create an IN sub-query");
		try {
			return (RexSubQuery) method.invoke(null, relNode, toImmutableList(rexNodes));
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to create RexSubQuery", e);
		}
	}

	// Process the position alias in GROUPBY and ORDERBY
	public static void processPositionAlias(SemanticAnalyzer analyzer, ASTNode node) {
		try {
			Method method = analyzer.getClass().getMethod("processPositionAlias", ASTNode.class);
			method.setAccessible(true);
			method.invoke(analyzer, node);
		} catch (Exception e) {
			throw new FlinkHiveException(e);
		}
	}

	/**
	 * Check if the table is the temporary table created by VALUES() syntax.
	 *
	 * @param tableName table name
	 */
	public static boolean isValuesTempTable(String tableName) {
		return tableName.toLowerCase().startsWith(HiveParserSemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase());
	}

	public static ReadEntity addInput(Set<ReadEntity> inputs, ReadEntity newInput, boolean mergeIsDirectFlag) {
		// If the input is already present, make sure the new parent is added to the input.
		if (inputs.contains(newInput)) {
			for (ReadEntity input : inputs) {
				if (input.equals(newInput)) {
					if ((newInput.getParents() != null) && (!newInput.getParents().isEmpty())) {
						input.getParents().addAll(newInput.getParents());
						input.setDirect(input.isDirect() || newInput.isDirect());
					} else if (mergeIsDirectFlag) {
						input.setDirect(input.isDirect() || newInput.isDirect());
					}
					return input;
				}
			}
			assert false;
		} else {
			inputs.add(newInput);
			return newInput;
		}
		// make compile happy
		return null;
	}

	public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr, HiveParserTypeCheckCtx tcCtx)
			throws SemanticException {
		return HiveParserTypeCheckProcFactory.genExprNode(expr, tcCtx, new HiveParserJoinCondTypeCheckProcFactory());
	}

	public static String generateErrorMessage(ASTNode ast, String message) {
		StringBuilder sb = new StringBuilder();
		if (ast == null) {
			sb.append(message).append(". Cannot tell the position of null AST.");
			return sb.toString();
		}
		sb.append(ast.getLine());
		sb.append(":");
		sb.append(ast.getCharPositionInLine());
		sb.append(" ");
		sb.append(message);
		sb.append(". Error encountered near token '");
		sb.append(ErrorMsg.getText(ast));
		sb.append("'");
		return sb.toString();
	}

	/**
	 * Convert a string to Text format and write its bytes in the same way TextOutputFormat would do.
	 * This is needed to properly encode non-ascii characters.
	 */
	public static void writeAsText(String text, FSDataOutputStream out) throws IOException {
		Text to = new Text(text);
		out.write(to.getBytes(), 0, to.getLength());
	}

	private static ASTNode buildSelExprSubTree(String tableAlias, String col) {
		ASTNode selexpr = new ASTNode(new CommonToken(HiveASTParser.TOK_SELEXPR, "TOK_SELEXPR"));
		ASTNode tableOrCol = new ASTNode(new CommonToken(HiveASTParser.TOK_TABLE_OR_COL,
				"TOK_TABLE_OR_COL"));
		ASTNode dot = new ASTNode(new CommonToken(HiveASTParser.DOT, "."));
		tableOrCol.addChild(new ASTNode(new CommonToken(HiveASTParser.Identifier, tableAlias)));
		dot.addChild(tableOrCol);
		dot.addChild(new ASTNode(new CommonToken(HiveASTParser.Identifier, col)));
		selexpr.addChild(dot);
		return selexpr;
	}

	public static ASTNode genSelectDIAST(HiveParserRowResolver rr) {
		LinkedHashMap<String, LinkedHashMap<String, ColumnInfo>> map = rr.getRslvMap();
		ASTNode selectDI = new ASTNode(new CommonToken(HiveASTParser.TOK_SELECTDI, "TOK_SELECTDI"));
		// Note: this will determine the order of columns in the result. For now, the columns for each
		//       table will be together; the order of the tables, as well as the columns within each
		//       table, is deterministic, but undefined - RR stores them in the order of addition.
		for (String tabAlias : map.keySet()) {
			for (Map.Entry<String, ColumnInfo> entry : map.get(tabAlias).entrySet()) {
				selectDI.addChild(buildSelExprSubTree(tabAlias, entry.getKey()));
			}
		}
		return selectDI;
	}

	public static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(
			GroupByDesc.Mode mode, boolean isDistinct) {
		switch (mode) {
			case COMPLETE:
				return GenericUDAFEvaluator.Mode.COMPLETE;
			case PARTIAL1:
				return GenericUDAFEvaluator.Mode.PARTIAL1;
			case PARTIAL2:
				return GenericUDAFEvaluator.Mode.PARTIAL2;
			case PARTIALS:
				return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1
						: GenericUDAFEvaluator.Mode.PARTIAL2;
			case FINAL:
				return GenericUDAFEvaluator.Mode.FINAL;
			case HASH:
				return GenericUDAFEvaluator.Mode.PARTIAL1;
			case MERGEPARTIAL:
				return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE
						: GenericUDAFEvaluator.Mode.FINAL;
			default:
				throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
		}
	}

	public static boolean isSkewedCol(String alias, HiveParserQB qb, String colName) {
		boolean isSkewedCol = false;
		List<String> skewedCols = qb.getSkewedColumnNames(alias);
		for (String skewedCol : skewedCols) {
			if (skewedCol.equalsIgnoreCase(colName)) {
				isSkewedCol = true;
			}
		}
		return isSkewedCol;
	}

	public static boolean isJoinToken(ASTNode node) {
		if ((node.getToken().getType() == HiveASTParser.TOK_JOIN)
				|| (node.getToken().getType() == HiveASTParser.TOK_CROSSJOIN)
				|| isOuterJoinToken(node)
				|| (node.getToken().getType() == HiveASTParser.TOK_LEFTSEMIJOIN)
				|| (node.getToken().getType() == HiveASTParser.TOK_UNIQUEJOIN)) {
			return true;
		}

		return false;
	}

	public static boolean isOuterJoinToken(ASTNode node) {
		return (node.getToken().getType() == HiveASTParser.TOK_LEFTOUTERJOIN)
				|| (node.getToken().getType() == HiveASTParser.TOK_RIGHTOUTERJOIN)
				|| (node.getToken().getType() == HiveASTParser.TOK_FULLOUTERJOIN);
	}

	public static void extractColumns(Set<String> colNamesExprs,
			ExprNodeDesc exprNode) throws SemanticException {
		if (exprNode instanceof ExprNodeColumnDesc) {
			colNamesExprs.add(((ExprNodeColumnDesc) exprNode).getColumn());
			return;
		}

		if (exprNode instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) exprNode;
			for (ExprNodeDesc childExpr : funcDesc.getChildren()) {
				extractColumns(colNamesExprs, childExpr);
			}
		}
	}

	public static boolean hasCommonElement(Set<String> set1, Set<String> set2) {
		for (String elem1 : set1) {
			if (set2.contains(elem1)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns the GenericUDAFInfo struct for the aggregation.
	 *
	 * @param evaluator
	 * @param emode
	 * @param aggParameters The exprNodeDesc of the original parameters
	 * @return GenericUDAFInfo
	 * @throws SemanticException when the UDAF is not found or has problems.
	 */
	public static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
			GenericUDAFEvaluator.Mode emode, ArrayList<ExprNodeDesc> aggParameters) throws SemanticException {

		GenericUDAFInfo res = new GenericUDAFInfo();

		// set r.genericUDAFEvaluator
		res.genericUDAFEvaluator = evaluator;

		// set r.returnType
		ObjectInspector returnOI = null;
		try {
			ArrayList<ObjectInspector> aggOIs = getWritableObjectInspector(aggParameters);
			ObjectInspector[] aggOIArray = new ObjectInspector[aggOIs.size()];
			for (int ii = 0; ii < aggOIs.size(); ++ii) {
				aggOIArray[ii] = aggOIs.get(ii);
			}
			returnOI = res.genericUDAFEvaluator.init(emode, aggOIArray);
			res.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
		} catch (HiveException e) {
			throw new SemanticException(e);
		}
		// set r.convertedParameters
		// TODO: type conversion
		res.convertedParameters = aggParameters;

		return res;
	}

	/**
	 * Convert exprNodeDesc array to ObjectInspector array.
	 */
	public static ArrayList<ObjectInspector> getWritableObjectInspector(ArrayList<ExprNodeDesc> exprs) {
		ArrayList<ObjectInspector> result = new ArrayList<>();
		for (ExprNodeDesc expr : exprs) {
			result.add(expr.getWritableObjectInspector());
		}
		return result;
	}

	// Returns the GenericUDAFEvaluator for the aggregation. This is called once for each GroupBy aggregation.
	// TODO: Requiring a GenericUDAFEvaluator means we only support hive UDAFs. Need to avoid this to support flink UDAFs.
	public static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName, ArrayList<ExprNodeDesc> aggParameters,
			ASTNode aggTree, boolean isDistinct, boolean isAllColumns, SqlOperatorTable opTable)
			throws SemanticException {
		ArrayList<ObjectInspector> originalParameterTypeInfos = getWritableObjectInspector(aggParameters);
		GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
				aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
		if (result == null) {
			// this happens for temp functions
			SqlOperator sqlOperator = getSqlOperator(aggName, opTable, SqlFunctionCategory.USER_DEFINED_FUNCTION);
			if (sqlOperator instanceof HiveAggSqlFunction) {
				HiveGenericUDAF hiveGenericUDAF = (HiveGenericUDAF) ((HiveAggSqlFunction) sqlOperator).makeFunction(
						new Object[0], new LogicalType[0]);
				result = hiveGenericUDAF.createEvaluator(originalParameterTypeInfos.toArray(new ObjectInspector[0]));
			}
		}
		if (null == result) {
			String reason = "Looking for UDAF Evaluator\"" + aggName
					+ "\" with parameters " + originalParameterTypeInfos;
			throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg((ASTNode) aggTree.getChild(0), reason));
		}
		return result;
	}

	/**
	 * Returns whether the pattern is a regex expression (instead of a normal
	 * string). Normal string is a string with all alphabets/digits and "_".
	 */
	public static boolean isRegex(String pattern, HiveConf conf) {
		String qIdSupport = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
		if ("column".equals(qIdSupport)) {
			return false;
		}
		for (int i = 0; i < pattern.length(); i++) {
			if (!Character.isLetterOrDigit(pattern.charAt(i))
					&& pattern.charAt(i) != '_') {
				return true;
			}
		}
		return false;
	}

	public static String[] getColAlias(ASTNode selExpr, String defaultName,
			HiveParserRowResolver inputRR, boolean includeFuncName, int colNum) {
		String colAlias = null;
		String tabAlias = null;
		String[] colRef = new String[2];

		//for queries with a windowing expressions, the selexpr may have a third child
		if (selExpr.getChildCount() == 2 ||
				(selExpr.getChildCount() == 3 &&
						selExpr.getChild(2).getType() == HiveASTParser.TOK_WINDOWSPEC)) {
			// return zz for "xx + yy AS zz"
			colAlias = unescapeIdentifier(selExpr.getChild(1).getText().toLowerCase());
			colRef[0] = tabAlias;
			colRef[1] = colAlias;
			return colRef;
		}

		ASTNode root = (ASTNode) selExpr.getChild(0);
		if (root.getType() == HiveASTParser.TOK_TABLE_OR_COL) {
			colAlias =
					unescapeIdentifier(root.getChild(0).getText().toLowerCase());
			colRef[0] = tabAlias;
			colRef[1] = colAlias;
			return colRef;
		}

		if (root.getType() == HiveASTParser.DOT) {
			ASTNode tab = (ASTNode) root.getChild(0);
			if (tab.getType() == HiveASTParser.TOK_TABLE_OR_COL) {
				String t = unescapeIdentifier(tab.getChild(0).getText());
				if (inputRR.hasTableAlias(t)) {
					tabAlias = t;
				}
			}

			// Return zz for "xx.zz" and "xx.yy.zz"
			ASTNode col = (ASTNode) root.getChild(1);
			if (col.getType() == HiveASTParser.Identifier) {
				colAlias = unescapeIdentifier(col.getText().toLowerCase());
			}
		}

		// if specified generate alias using func name
		if (includeFuncName && (root.getType() == HiveASTParser.TOK_FUNCTION)) {

			String exprFlattened = root.toStringTree();

			// remove all TOK tokens
			String exprNoTok = exprFlattened.replaceAll("tok_\\S+", "");

			// remove all non alphanumeric letters, replace whitespace spans with underscore
			String exprFormatted = exprNoTok.replaceAll("\\W", " ").trim().replaceAll("\\s+", "_");

			// limit length to 20 chars
			if (exprFormatted.length() > HiveParserSemanticAnalyzer.AUTOGEN_COLALIAS_PRFX_MAXLENGTH) {
				exprFormatted = exprFormatted.substring(0, HiveParserSemanticAnalyzer.AUTOGEN_COLALIAS_PRFX_MAXLENGTH);
			}

			// append colnum to make it unique
			colAlias = exprFormatted.concat("_" + colNum);
		}

		if (colAlias == null) {
			// Return defaultName if selExpr is not a simple xx.yy.zz
			colAlias = defaultName + colNum;
		}

		colRef[0] = tabAlias;
		colRef[1] = colAlias;
		return colRef;
	}

	public static int unsetBit(int bitmap, int bitIdx) {
		return bitmap & ~(1 << bitIdx);
	}

	public static ASTNode rewriteGroupingFunctionAST(final List<ASTNode> grpByAstExprs, ASTNode targetNode,
			final boolean noneSet) throws SemanticException {
		final MutableBoolean visited = new MutableBoolean(false);
		final MutableBoolean found = new MutableBoolean(false);

		TreeVisitorAction action = new TreeVisitorAction() {

			@Override
			public Object pre(Object t) {
				return t;
			}

			@Override
			public Object post(Object t) {
				ASTNode root = (ASTNode) t;
				if (root.getType() == HiveASTParser.TOK_FUNCTION && root.getChildCount() == 2) {
					ASTNode func = (ASTNode) HiveASTParseDriver.ADAPTOR.getChild(root, 0);
					if (func.getText().equals("grouping")) {
						ASTNode c = (ASTNode) HiveASTParseDriver.ADAPTOR.getChild(root, 1);
						visited.setValue(true);
						for (int i = 0; i < grpByAstExprs.size(); i++) {
							ASTNode grpByExpr = grpByAstExprs.get(i);
							if (grpByExpr.toStringTree().equals(c.toStringTree())) {
								ASTNode child1;
								if (noneSet) {
									// Query does not contain CUBE, ROLLUP, or GROUPING SETS, and thus,
									// grouping should return 0
									child1 = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.IntegralLiteral,
											String.valueOf(0));
								} else {
									// We refer to grouping_id column
									child1 = (ASTNode) HiveASTParseDriver.ADAPTOR.create(
											HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
									HiveASTParseDriver.ADAPTOR.addChild(child1, HiveASTParseDriver.ADAPTOR.create(
											HiveASTParser.Identifier, VirtualColumn.GROUPINGID.getName()));
								}
								ASTNode child2 = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.IntegralLiteral,
										String.valueOf(com.google.common.math.IntMath.mod(-i - 1, grpByAstExprs.size())));
								root.setChild(1, child1);
								root.addChild(child2);
								found.setValue(true);
								break;
							}
						}
					}
				}
				return t;
			}
		};
		ASTNode newTargetNode = (ASTNode) new TreeVisitor(HiveASTParseDriver.ADAPTOR).visit(targetNode, action);
		if (visited.booleanValue() && !found.booleanValue()) {
			throw new SemanticException("Expression in GROUPING function not present in GROUP BY");
		}
		return newTargetNode;
	}

	public static SqlOperator getAnySqlOperator(String funcName, SqlOperatorTable opTable) {
		SqlOperator sqlOperator = getSqlOperator(funcName, opTable, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		if (sqlOperator == null) {
			sqlOperator = getSqlOperator(funcName, opTable, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
		}
		return sqlOperator;
	}

	public static SqlOperator getSqlOperator(String funcName, SqlOperatorTable opTable, SqlFunctionCategory category) {
		funcName = funcName.toLowerCase();
		String[] names = funcName.split("\\.");
		SqlIdentifier identifier = new SqlIdentifier(Arrays.asList(names), SqlParserPos.ZERO);
		List<SqlOperator> operators = new ArrayList<>();
		try {
			opTable.lookupOperatorOverloads(identifier, category, SqlSyntax.FUNCTION, operators, SqlNameMatchers.withCaseSensitive(false));
		} catch (Exception e) {
			LOG.warn("Error trying to resolve function " + funcName, e);
		}
		if (operators.isEmpty()) {
			return null;
		} else {
			return operators.get(0);
		}
	}

	public static RelDataType inferReturnTypeForOperandsTypes(SqlOperator sqlOperator, List<RelDataType> types,
			List<RexNode> operands, RelDataTypeFactory dataTypeFactory) {
		HiveParserOperatorBinding operatorBinding = new HiveParserOperatorBinding(dataTypeFactory, sqlOperator, types, operands);
		if (sqlOperator instanceof BridgingSqlFunction || sqlOperator instanceof HiveAggSqlFunction) {
			SqlReturnTypeInference returnTypeInference = sqlOperator.getReturnTypeInference();
			return returnTypeInference.inferReturnType(operatorBinding);
		} else if (sqlOperator instanceof HiveTableSqlFunction) {
			HiveGenericUDTF hiveGenericUDTF = (HiveGenericUDTF) ((HiveTableSqlFunction) sqlOperator).makeFunction(new Object[0], new LogicalType[0]);
			DataType dataType = hiveGenericUDTF.getHiveResultType(
					operatorBinding.getConstantOperands(),
					types.stream().map(HiveParserUtils::toDataType).toArray(DataType[]::new));
			return toRelDataType(dataType, dataTypeFactory);
		} else {
			throw new FlinkHiveException("Unsupported SqlOperator class " + sqlOperator.getClass().getName());
		}
	}

	public static RelDataType inferReturnTypeForOperands(SqlOperator sqlOperator, List<RexNode> operands,
			RelDataTypeFactory dataTypeFactory) {
		return inferReturnTypeForOperandsTypes(
				sqlOperator,
				operands.stream().map(RexNode::getType).collect(Collectors.toList()),
				operands,
				dataTypeFactory);
	}

	private static LogicalType[] toLogicalTypes(RelDataType[] relDataTypes) {
		LogicalType[] res = new LogicalType[relDataTypes.length];
		for (int i = 0; i < res.length; i++) {
			res[i] = toDataType(relDataTypes[i]).getLogicalType();
		}
		return res;
	}

	public static RelDataType toRelDataType(DataType dataType, RelDataTypeFactory dtFactory) {
		try {
			return toRelDataType(HiveTypeUtil.toHiveTypeInfo(dataType, false), dtFactory);
		} catch (SemanticException e) {
			throw new FlinkHiveException(e);
		}
	}

	public static DataType toDataType(RelDataType relDataType) {
		return HiveTypeUtil.toFlinkType(HiveParserTypeConverter.convert(relDataType));
	}

	// extracts useful information for a given lateral view node
	public static LateralViewInfo extractLateralViewInfo(ASTNode lateralView, HiveParserRowResolver inputRR,
			HiveParserSemanticAnalyzer hiveAnalyzer, FrameworkConfig frameworkConfig, RelOptCluster cluster) throws SemanticException {
		// checks the left sub-tree
		ASTNode sel = (ASTNode) lateralView.getChild(0);
		Preconditions.checkArgument(sel.getToken().getType() == HiveASTParser.TOK_SELECT);
		Preconditions.checkArgument(sel.getChildCount() == 1);
		ASTNode selExpr = (ASTNode) sel.getChild(0);
		Preconditions.checkArgument(selExpr.getToken().getType() == HiveASTParser.TOK_SELEXPR);
		// decide function name and function
		ASTNode func = (ASTNode) selExpr.getChild(0);
		Preconditions.checkArgument(func.getToken().getType() == HiveASTParser.TOK_FUNCTION);
		String funcName = getFunctionText(func, true);
		SqlOperator sqlOperator = getSqlOperator(funcName, frameworkConfig.getOperatorTable(), SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
		Preconditions.checkArgument(isUDTF(sqlOperator), funcName + " is not a valid UDTF");
		// decide operands
		List<ExprNodeDesc> operands = new ArrayList<>(func.getChildCount() - 1);
		List<ColumnInfo> operandColInfos = new ArrayList<>(func.getChildCount() - 1);
		HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(inputRR, frameworkConfig, cluster);
		for (int i = 1; i < func.getChildCount(); i++) {
			ExprNodeDesc exprDesc = hiveAnalyzer.genExprNodeDesc((ASTNode) func.getChild(i), inputRR, typeCheckCtx);
			operands.add(exprDesc);
			operandColInfos.add(new ColumnInfo(getColumnInternalName(i - 1), exprDesc.getWritableObjectInspector(), null, false));
		}
		// decide table alias -- there must be a table alias
		ASTNode tabAliasNode = (ASTNode) selExpr.getChild(selExpr.getChildCount() - 1);
		Preconditions.checkArgument(tabAliasNode.getToken().getType() == HiveASTParser.TOK_TABALIAS);
		String tabAlias = unescapeIdentifier(tabAliasNode.getChild(0).getText().toLowerCase());
		// decide column aliases -- column aliases are optional
		List<String> colAliases = new ArrayList<>();
		for (int i = 1; i < selExpr.getChildCount() - 1; i++) {
			ASTNode child = (ASTNode) selExpr.getChild(i);
			Preconditions.checkArgument(child.getToken().getType() == HiveASTParser.Identifier);
			colAliases.add(unescapeIdentifier(child.getText().toLowerCase()));
		}
		return new LateralViewInfo(funcName, sqlOperator, operands, operandColInfos, colAliases, tabAlias);
	}

	public static boolean isUDAF(SqlOperator sqlOperator) {
		return sqlOperator instanceof SqlAggFunction;
	}

	public static boolean isUDTF(SqlOperator sqlOperator) {
		if (sqlOperator instanceof BridgingSqlFunction) {
			return ((BridgingSqlFunction) sqlOperator).getDefinition().getKind() == FunctionKind.TABLE;
		} else {
			return sqlOperator instanceof SqlUserDefinedTableFunction;
		}
	}

	// TODO: we need a way to tell whether a function is built-in, for now just return false so that the unparser will quote them
	public static boolean isNative(SqlOperator sqlOperator) {
		return false;
	}

	/**
	 * Information needed to generate logical plan for a lateral view.
	 */
	public static class LateralViewInfo {
		private final String funcName;
		private final SqlOperator sqlOperator;
		// operands to the UDTF
		private final List<ExprNodeDesc> operands;
		private final List<ColumnInfo> operandColInfos;
		// aliases for the UDTF output
		private final List<String> colAliases;
		// alias of the logical table
		private final String tabAlias;

		public LateralViewInfo(String funcName, SqlOperator sqlOperator, List<ExprNodeDesc> operands,
				List<ColumnInfo> operandColInfos, List<String> colAliases, String tabAlias) {
			this.funcName = funcName;
			this.sqlOperator = sqlOperator;
			this.operands = operands;
			this.operandColInfos = operandColInfos;
			this.colAliases = colAliases;
			this.tabAlias = tabAlias;
		}

		public String getFuncName() {
			return funcName;
		}

		public SqlOperator getSqlOperator() {
			return sqlOperator;
		}

		public List<ExprNodeDesc> getOperands() {
			return operands;
		}

		public List<ColumnInfo> getOperandColInfos() {
			return operandColInfos;
		}

		public List<String> getColAliases() {
			return colAliases;
		}

		public String getTabAlias() {
			return tabAlias;
		}
	}

	public static List<String> splitSQLStatements(String statements) {
		String[] lines = statements.split("\\R");
		StringBuilder builder = new StringBuilder();
		for (String line : lines) {
			line = line.trim();
			if (line.startsWith("--")) {
				continue;
			}
			if (builder.length() > 0) {
				builder.append("\n");
			}
			builder.append(line);
		}
		return splitSemiColon(builder.toString());
	}

	private static List<String> splitSemiColon(String line) {
		boolean insideSingleQuote = false;
		boolean insideDoubleQuote = false;
		boolean escape = false;
		int beginIndex = 0;
		List<String> ret = new ArrayList<>();
		for (int index = 0; index < line.length(); index++) {
			if (line.startsWith("--", index)) {
				if (!insideSingleQuote && !insideDoubleQuote && !escape) {
					// skip until new line
					index += 2;
					while (index < line.length() && line.charAt(index) != '\n') {
						index++;
					}
					if (index == line.length()) {
						break;
					}
				}
			} else if (line.charAt(index) == '\'') {
				// take a look to see if it is escaped
				if (!escape) {
					// flip the boolean variable
					insideSingleQuote = !insideSingleQuote;
				}
			} else if (line.charAt(index) == '\"') {
				// take a look to see if it is escaped
				if (!escape) {
					// flip the boolean variable
					insideDoubleQuote = !insideDoubleQuote;
				}
			} else if (line.charAt(index) == ';') {
				if (!insideSingleQuote && !insideDoubleQuote && !escape) {
					// split, do not include ; itself
					ret.add(line.substring(beginIndex, index));
					beginIndex = index + 1;
				}
			}
			// set the escape
			if (escape) {
				escape = false;
			} else if (line.charAt(index) == '\\') {
				escape = true;
			}
		}
		ret.add(line.substring(beginIndex));
		return ret.stream().map(String::trim).filter(s -> !StringUtils.isNullOrWhitespaceOnly(s)).collect(Collectors.toList());
	}

	/**
	 * Push any equi join conditions that are not column references as Projections
	 * on top of the children.
	 *
	 * @param factory        Project factory to use.
	 * @param inputRels      inputs to a join
	 * @param leftJoinKeys   expressions for LHS of join key
	 * @param rightJoinKeys  expressions for RHS of join key
	 * @param systemColCount number of system columns, usually zero. These columns are
	 *                       projected at the leading edge of the output row.
	 * @param leftKeys       on return this contains the join key positions from the new
	 *                       project rel on the LHS.
	 * @param rightKeys      on return this contains the join key positions from the new
	 *                       project rel on the RHS.
	 * @return the join condition after the equi expressions pushed down.
	 */
	public static RexNode projectNonColumnEquiConditions(RelFactories.ProjectFactory factory, RelNode[] inputRels,
			List<RexNode> leftJoinKeys, List<RexNode> rightJoinKeys, int systemColCount,
			List<Integer> leftKeys, List<Integer> rightKeys) {
		RelNode leftRel = inputRels[0];
		RelNode rightRel = inputRels[1];
		RexBuilder rexBuilder = leftRel.getCluster().getRexBuilder();
		RexNode outJoinCond = null;

		int origLeftInputSize = leftRel.getRowType().getFieldCount();
		int origRightInputSize = rightRel.getRowType().getFieldCount();

		List<RexNode> newLeftFields = new ArrayList<>();
		List<String> newLeftFieldNames = new ArrayList<>();

		List<RexNode> newRightFields = new ArrayList<>();
		List<String> newRightFieldNames = new ArrayList<>();
		int leftKeyCount = leftJoinKeys.size();
		int i;

		for (i = 0; i < origLeftInputSize; i++) {
			final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
			newLeftFields.add(rexBuilder.makeInputRef(field.getType(), i));
			newLeftFieldNames.add(field.getName());
		}

		for (i = 0; i < origRightInputSize; i++) {
			final RelDataTypeField field = rightRel.getRowType().getFieldList().get(i);
			newRightFields.add(rexBuilder.makeInputRef(field.getType(), i));
			newRightFieldNames.add(field.getName());
		}

		ImmutableBitSet.Builder origColEqCondsPosBuilder = ImmutableBitSet.builder();
		int newKeyCount = 0;
		List<Pair<Integer, Integer>> origColEqConds = new ArrayList<>();
		for (i = 0; i < leftKeyCount; i++) {
			RexNode leftKey = leftJoinKeys.get(i);
			RexNode rightKey = rightJoinKeys.get(i);

			if (leftKey instanceof RexInputRef && rightKey instanceof RexInputRef) {
				origColEqConds.add(Pair.of(((RexInputRef) leftKey).getIndex(),
						((RexInputRef) rightKey).getIndex()));
				origColEqCondsPosBuilder.set(i);
			} else {
				newLeftFields.add(leftKey);
				newLeftFieldNames.add(null);
				newRightFields.add(rightKey);
				newRightFieldNames.add(null);
				newKeyCount++;
			}
		}
		ImmutableBitSet origColEqCondsPos = origColEqCondsPosBuilder.build();

		for (i = 0; i < origColEqConds.size(); i++) {
			Pair<Integer, Integer> p = origColEqConds.get(i);
			int condPos = origColEqCondsPos.nth(i);
			RexNode leftKey = leftJoinKeys.get(condPos);
			RexNode rightKey = rightJoinKeys.get(condPos);
			leftKeys.add(p.left);
			rightKeys.add(p.right);
			RexNode cond = rexBuilder.makeCall(
					SqlStdOperatorTable.EQUALS,
					rexBuilder.makeInputRef(leftKey.getType(), systemColCount + p.left),
					rexBuilder.makeInputRef(rightKey.getType(), systemColCount + origLeftInputSize
							+ newKeyCount + p.right));
			if (outJoinCond == null) {
				outJoinCond = cond;
			} else {
				outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond, cond);
			}
		}

		if (newKeyCount == 0) {
			return outJoinCond;
		}

		int newLeftOffset = systemColCount + origLeftInputSize;
		int newRightOffset = systemColCount + origLeftInputSize + origRightInputSize + newKeyCount;
		for (i = 0; i < newKeyCount; i++) {
			leftKeys.add(origLeftInputSize + i);
			rightKeys.add(origRightInputSize + i);
			RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
					rexBuilder.makeInputRef(newLeftFields.get(origLeftInputSize + i).getType(), newLeftOffset + i),
					rexBuilder.makeInputRef(newRightFields.get(origRightInputSize + i).getType(), newRightOffset + i));
			if (outJoinCond == null) {
				outJoinCond = cond;
			} else {
				outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond, cond);
			}
		}

		// added project if need to produce new keys than the original input
		// fields
		if (newKeyCount > 0) {
			leftRel = factory.createProject(leftRel, Collections.emptyList(), newLeftFields,
					SqlValidatorUtil.uniquify(newLeftFieldNames, false));
			rightRel = factory.createProject(rightRel, Collections.emptyList(), newRightFields,
					SqlValidatorUtil.uniquify(newRightFieldNames, false));
		}

		inputRels[0] = leftRel;
		inputRels[1] = rightRel;

		return outJoinCond;
	}

	public static List<RexNode> getProjsFromBelowAsInputRef(final RelNode rel) {
		return rel.getRowType().getFieldList().stream()
				.map(field -> rel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex()))
				.collect(Collectors.toList());
	}

	public static boolean pivotResult(String functionName) throws SemanticException {
		WindowFunctionInfo windowInfo = FunctionRegistry.getWindowFunctionInfo(functionName);
		if (windowInfo != null) {
			return windowInfo.isPivotResult();
		}
		return false;
	}

	// Get FunctionInfo and always look for it in metastore when FunctionRegistry returns null.
	public static FunctionInfo getFunctionInfo(String funcName) throws SemanticException {
		FunctionInfo res = FunctionRegistry.getFunctionInfo(funcName);
		if (res == null) {
			SessionState sessionState = SessionState.get();
			HiveConf hiveConf = sessionState != null ? sessionState.getConf() : null;
			if (hiveConf != null) {
				// TODO: need to support overriding hive version
				try (HiveMetastoreClientWrapper hmsClient = new HiveMetastoreClientWrapper(hiveConf, HiveShimLoader.getHiveVersion())) {
					String[] parts = FunctionUtils.getQualifiedFunctionNameParts(funcName);
					Function function = hmsClient.getFunction(parts[0], parts[1]);
					getSessionHiveShim().registerTemporaryFunction(
							FunctionUtils.qualifyFunctionName(parts[1], parts[0]),
							Thread.currentThread().getContextClassLoader().loadClass(function.getClassName()));
					res = FunctionRegistry.getFunctionInfo(funcName);
				} catch (NoSuchObjectException e) {
					LOG.warn("Function {} doesn't exist in metastore", funcName);
				} catch (Exception e) {
					LOG.warn("Failed to look up function in metastore", e);
				}
			}
		}
		return res;
	}

	public static List<FieldSchema> convertRowSchemaToResultSetSchema(HiveParserRowResolver rr,
			boolean useTabAliasIfAvailable) {
		List<FieldSchema> fieldSchemas = new ArrayList<>();
		String[] qualifiedColName;
		String colName;

		for (ColumnInfo colInfo : rr.getColumnInfos()) {
			if (colInfo.isHiddenVirtualCol()) {
				continue;
			}

			qualifiedColName = rr.reverseLookup(colInfo.getInternalName());
			if (useTabAliasIfAvailable && qualifiedColName[0] != null && !qualifiedColName[0].isEmpty()) {
				colName = qualifiedColName[0] + "." + qualifiedColName[1];
			} else {
				colName = qualifiedColName[1];
			}
			fieldSchemas.add(new FieldSchema(colName, colInfo.getType().getTypeName(), null));
		}
		return fieldSchemas;
	}

	public static void saveViewDefinition(List<FieldSchema> resultSchema, HiveParserCreateViewDesc desc,
			TokenRewriteStream tokenRewriteStream, HiveParserUnparseTranslator unparseTranslator, HiveConf conf) throws SemanticException {
		// Make a copy of the statement's result schema, since we may
		// modify it below as part of imposing view column names.
		List<FieldSchema> derivedSchema = new ArrayList<>(resultSchema);
		ParseUtils.validateColumnNameUniqueness(derivedSchema);

		List<FieldSchema> imposedSchema = desc.getSchema();
		if (imposedSchema != null) {
			int explicitColCount = imposedSchema.size();
			int derivedColCount = derivedSchema.size();
			if (explicitColCount != derivedColCount) {
				throw new SemanticException(generateErrorMessage(
						desc.getQuery(),
						ErrorMsg.VIEW_COL_MISMATCH.getMsg()));
			}
		}

		// Preserve the original view definition as specified by the user.
		if (desc.getOriginalText() == null) {
			String originalText = tokenRewriteStream.toString(
					desc.getQuery().getTokenStartIndex(), desc.getQuery().getTokenStopIndex());
			desc.setOriginalText(originalText);
		}

		// Now expand the view definition with extras such as explicit column
		// references; this expanded form is what we'll re-parse when the view is
		// referenced later.
		unparseTranslator.applyTranslations(tokenRewriteStream);
		String expandedText = tokenRewriteStream.toString(
				desc.getQuery().getTokenStartIndex(), desc.getQuery().getTokenStopIndex());

		if (imposedSchema != null) {
			// Merge the names from the imposed schema into the types
			// from the derived schema.
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT ");
			int n = derivedSchema.size();
			for (int i = 0; i < n; ++i) {
				if (i > 0) {
					sb.append(", ");
				}
				FieldSchema fieldSchema = derivedSchema.get(i);
				// Modify a copy, not the original
				fieldSchema = new FieldSchema(fieldSchema);
				// TODO: there's a potential problem here if some table uses external schema like Avro,
				//       with a very large type name. It seems like the view does not derive the SerDe from
				//       the table, so it won't be able to just get the type from the deserializer like the
				//       table does; we won't be able to properly store the type in the RDBMS metastore.
				//       Not sure if these large cols could be in resultSchema. Ignore this for now 0_o
				derivedSchema.set(i, fieldSchema);
				sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
				sb.append(" AS ");
				String imposedName = imposedSchema.get(i).getName();
				sb.append(HiveUtils.unparseIdentifier(imposedName, conf));
				fieldSchema.setName(imposedName);
				// We don't currently allow imposition of a type
				fieldSchema.setComment(imposedSchema.get(i).getComment());
			}
			sb.append(" FROM (");
			sb.append(expandedText);
			sb.append(") ");
			sb.append(HiveUtils.unparseIdentifier(desc.getCompoundName(), conf));
			expandedText = sb.toString();
		}

		desc.setSchema(derivedSchema);
		if (!desc.isMaterialized()) {
			// materialized views don't store the expanded text as they won't be rewritten at query time.
			desc.setExpandedText(expandedText);
		}
	}

	public static HiveShim getSessionHiveShim() {
		return HiveShimLoader.loadHiveShim(SessionState.get().getConf().get(HiveCatalogValidator.CATALOG_HIVE_VERSION));
	}

	public static String getStandardDisplayString(String name, String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append("(");
		if (children.length > 0) {
			sb.append(children[0]);
			for (int i = 1; i < children.length; i++) {
				sb.append(", ");
				sb.append(children[i]);
			}
		}
		sb.append(")");
		return sb.toString();
	}

	/**
	 * Creates the directory and all necessary parent directories.
	 * @param fs FileSystem to use
	 * @param f path to create.
	 * @param inheritPerms whether directory inherits the permission of the last-existing parent path
	 * @param conf Hive configuration
	 * @return true if directory created successfully.  False otherwise, including if it exists.
	 * @throws IOException exception in creating the directory
	 */
	public static boolean mkdir(FileSystem fs, Path f, boolean inheritPerms, Configuration conf) throws IOException {
		LOG.info("Creating directory if it doesn't exist: " + f);
		if (!inheritPerms) {
			//just create the directory
			return fs.mkdirs(f);
		} else {
			//Check if the directory already exists. We want to change the permission
			//to that of the parent directory only for newly created directories.
			try {
				return fs.getFileStatus(f).isDir();
			} catch (FileNotFoundException ignore) {
			}
			// inherit perms: need to find last existing parent path, and apply its permission on entire subtree.
			Path lastExistingParent = f;
			Path firstNonExistentParent = null;
			while (!fs.exists(lastExistingParent)) {
				firstNonExistentParent = lastExistingParent;
				lastExistingParent = lastExistingParent.getParent();
			}
			boolean success = fs.mkdirs(f);
			if (!success) {
				return false;
			} else {
				//set on the entire subtree
				HdfsUtils.setFullFileStatus(conf,
						new HdfsUtils.HadoopFileStatus(conf, fs, lastExistingParent), fs,
						firstNonExistentParent, true);
				return true;
			}
		}
	}

	/**
	 * A visitor to collect correlation IDs and required columns.
	 */
	public static class CorrelationCollector extends RexVisitorImpl<Void> {
		private final List<CorrelationId> correlIDs;
		private final ImmutableBitSet.Builder requiredColumns;

		public CorrelationCollector(List<CorrelationId> correlIDs, ImmutableBitSet.Builder requiredColumns) {
			super(true);
			this.correlIDs = correlIDs;
			this.requiredColumns = requiredColumns;
		}

		@Override
		public Void visitFieldAccess(RexFieldAccess fieldAccess) {
			RexNode expr = fieldAccess.getReferenceExpr();
			if (expr instanceof RexCorrelVariable) {
				requiredColumns.set(fieldAccess.getField().getIndex());
			}
			return super.visitFieldAccess(fieldAccess);
		}

		@Override
		public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
			correlIDs.add(correlVariable.id);
			return null;
		}
	}

	/**
	 * A bit both of ExplicitOperatorBinding and RexCallBinding.
	 */
	private static class HiveParserOperatorBinding extends ExplicitOperatorBinding {

		// can contain null for non-literal operand
		private final List<RexNode> operands;

		public HiveParserOperatorBinding(RelDataTypeFactory typeFactory, SqlOperator operator, List<RelDataType> types, List<RexNode> operands) {
			super(typeFactory, operator, types);
			this.operands = Preconditions.checkNotNull(operands, "Operands cannot be null");
			Preconditions.checkArgument(types.size() == operands.size(),
					String.format("Type length %d and operand length %d mismatch", types.size(), operands.size()));
		}

		@Override
		public String getStringLiteralOperand(int ordinal) {
			return RexLiteral.stringValue(operands.get(ordinal));
		}

		@Override
		public int getIntLiteralOperand(int ordinal) {
			return RexLiteral.intValue(operands.get(ordinal));
		}

		@Override
		public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
			RexNode operand = operands.get(ordinal);
			if (operand instanceof RexLiteral) {
				return ((RexLiteral) operand).getValueAs(clazz);
			}
			throw new AssertionError("not a literal: " + operand);
		}

		@Override
		public boolean isOperandLiteral(int ordinal, boolean allowCast) {
			RexNode operand = operands.get(ordinal);
			// we never consider cast as literal, because hive udf will convert char/varchar literals to string type,
			// so we need to differentiate cast from a real literal
			return operand != null && RexUtil.isLiteral(operand, false);
		}

		@Override
		public boolean isOperandNull(int ordinal, boolean allowCast) {
			RexNode operand = operands.get(ordinal);
			// we never consider cast as literal, because hive udf will convert char/varchar literals to string type,
			// so we need to differentiate cast from a real literal
			return operand != null && RexUtil.isNullLiteral(operand, false);
		}

		public Object[] getConstantOperands() {
			Object[] res = new Object[operands.size()];
			for (int i = 0; i < res.length; i++) {
				if (isOperandLiteral(i, false)) {
					res[i] = getOperandLiteralValue(i, ClassLogicalTypeConverter.getDefaultExternalClassForType(
							FlinkTypeFactory.toLogicalType(getOperandType(i))));
				}
			}
			return res;
		}
	}
}
