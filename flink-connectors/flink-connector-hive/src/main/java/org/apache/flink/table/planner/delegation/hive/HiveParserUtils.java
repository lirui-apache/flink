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
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.util.Preconditions;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserJoinCondTypeCheckProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserTypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveASTParseDriver;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;
import org.apache.hadoop.hive.ql.parse.HiveParserQB;
import org.apache.hadoop.hive.ql.parse.HiveParserRowResolver;
import org.apache.hadoop.hive.ql.parse.HiveParserSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParserTypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.HiveParserTypeCheckProcFactory;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.HiveParserSemanticAnalyzer.getColumnInternalName;
import static org.apache.hadoop.hive.ql.parse.HiveParserTypeCheckProcFactory.DefaultExprProcessor.getFunctionText;

/**
 * Util class for the hive planner.
 */
public class HiveParserUtils {

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
			throws CalciteSemanticException {
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
				throw new CalciteSemanticException(String.format("%s type is not supported yet", typeInfo.getCategory().name()));
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
	public static SemanticAnalyzer.GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
			GenericUDAFEvaluator.Mode emode, ArrayList<ExprNodeDesc> aggParameters)
			throws SemanticException {

		SemanticAnalyzer.GenericUDAFInfo res = new SemanticAnalyzer.GenericUDAFInfo();

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

	/**
	 * Returns the GenericUDAFEvaluator for the aggregation. This is called once
	 * for each GroupBy aggregation.
	 */
	public static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName,
			ArrayList<ExprNodeDesc> aggParameters, ASTNode aggTree,
			boolean isDistinct, boolean isAllColumns)
			throws SemanticException {
		ArrayList<ObjectInspector> originalParameterTypeInfos =
				getWritableObjectInspector(aggParameters);
		GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
				aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
		if (null == result) {
			String reason = "Looking for UDAF Evaluator\"" + aggName
					+ "\" with parameters " + originalParameterTypeInfos;
			throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
					(ASTNode) aggTree.getChild(0), reason));
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

	// extracts useful information for a given lateral view node
	public static LateralViewInfo extractLateralViewInfo(ASTNode lateralView, HiveParserRowResolver inputRR,
			HiveParserSemanticAnalyzer hiveAnalyzer, RelOptCluster cluster) throws SemanticException {
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
		FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
		GenericUDTF udtf = fi.getGenericUDTF();
		Preconditions.checkArgument(udtf != null, funcName + " is not a valid UDTF");
		// decide operands
		List<ExprNodeDesc> operands = new ArrayList<>(func.getChildCount() - 1);
		List<ColumnInfo> operandColInfos = new ArrayList<>(func.getChildCount() - 1);
		HiveParserTypeCheckCtx typeCheckCtx = new HiveParserTypeCheckCtx(inputRR);
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
		return new LateralViewInfo(funcName, udtf, operands, operandColInfos, colAliases, tabAlias);
	}

	/**
	 * Information needed to generate logical plan for a lateral view.
	 */
	public static class LateralViewInfo {
		private final String funcName;
		private final GenericUDTF func;
		// operands to the UDTF
		private final List<ExprNodeDesc> operands;
		private final List<ColumnInfo> operandColInfos;
		// aliases for the UDTF output
		private final List<String> colAliases;
		// alias of the logical table
		private final String tabAlias;

		public LateralViewInfo(String funcName, GenericUDTF func, List<ExprNodeDesc> operands,
				List<ColumnInfo> operandColInfos, List<String> colAliases, String tabAlias) {
			this.funcName = funcName;
			this.func = func;
			this.operands = operands;
			this.operandColInfos = operandColInfos;
			this.colAliases = colAliases;
			this.tabAlias = tabAlias;
		}

		public String getFuncName() {
			return funcName;
		}

		public GenericUDTF getFunc() {
			return func;
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
}
