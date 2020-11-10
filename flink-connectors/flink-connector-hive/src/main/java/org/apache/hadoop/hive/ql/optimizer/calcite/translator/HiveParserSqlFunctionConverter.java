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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.flink.table.planner.delegation.hive.HiveParserIN;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveParserSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveParserSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveParserExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveParserFloorDate;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveASTParseDriver;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPositive;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Counterpart of hive's SqlFunctionConverter.
 */
public class HiveParserSqlFunctionConverter {

	private static final Logger LOG = LoggerFactory.getLogger(HiveParserSqlFunctionConverter.class);

	static final Map<String, SqlOperator> HIVE_TO_CALCITE;
	static final Map<SqlOperator, HiveToken> CALCITE_TO_HIVE_TOKEN;
	static final Map<SqlOperator, String> REVERSE_OPERATOR_MAP;

	static {
		StaticBlockBuilder builder = new StaticBlockBuilder();
		HIVE_TO_CALCITE = Collections.unmodifiableMap(builder.hiveToCalcite);
		CALCITE_TO_HIVE_TOKEN = Collections.unmodifiableMap(builder.calciteToHiveToken);
		REVERSE_OPERATOR_MAP = Collections.unmodifiableMap(builder.reverseOperatorMap);
	}

	public static SqlOperator getCalciteOperator(String funcTextName, GenericUDF hiveUDF,
			List<RelDataType> calciteArgTypes, RelDataType retType)
			throws SemanticException {
		// handle overloaded methods first
		if (hiveUDF instanceof GenericUDFOPNegative) {
			return SqlStdOperatorTable.UNARY_MINUS;
		} else if (hiveUDF instanceof GenericUDFOPPositive) {
			return SqlStdOperatorTable.UNARY_PLUS;
		} // do generic lookup
		String name = null;
		if (StringUtils.isEmpty(funcTextName)) {
			name = getName(hiveUDF); // this should probably never happen, see
			// getName
			// comment
			LOG.warn("The function text was empty, name from annotation is " + name);
		} else {
			// We could just do toLowerCase here and let SA qualify it, but
			// let's be proper...
			name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
		}
		return getCalciteFn(name, calciteArgTypes, retType, FunctionRegistry.isDeterministic(hiveUDF));
	}

	public static SqlOperator getCalciteOperator(String funcTextName, GenericUDTF hiveUDTF,
			List<RelDataType> calciteArgTypes, RelDataType retType) throws SemanticException {
		// We could just do toLowerCase here and let SA qualify it, but
		// let's be proper...
		String name = FunctionRegistry.getNormalizedFunctionName(funcTextName);
		return getCalciteFn(name, calciteArgTypes, retType, false);
	}

//	public static GenericUDF getHiveUDF(SqlOperator op, RelDataType dt, int argsLength) {
//		String name = REVERSE_OPERATOR_MAP.get(op);
//		if (name == null) {
//			name = op.getName();
//		}
//		// Make sure we handle unary + and - correctly.
//		if (argsLength == 1) {
//			if (name == "+") {
//				name = FunctionRegistry.UNARY_PLUS_FUNC_NAME;
//			} else if (name == "-") {
//				name = FunctionRegistry.UNARY_MINUS_FUNC_NAME;
//			}
//		}
//		FunctionInfo hFn;
//		try {
//			hFn = name != null ? FunctionRegistry.getFunctionInfo(name) : null;
//		} catch (SemanticException e) {
//			LOG.warn("Failed to load udf " + name, e);
//			hFn = null;
//		}
//		if (hFn == null) {
//			try {
//				hFn = handleExplicitCast(op, dt);
//			} catch (SemanticException e) {
//				LOG.warn("Failed to load udf " + name, e);
//				hFn = null;
//			}
//		}
//		return hFn == null ? null : hFn.getGenericUDF();
//	}

//	private static FunctionInfo handleExplicitCast(SqlOperator op, RelDataType dt)
//			throws SemanticException {
//		FunctionInfo castUDF = null;
//
//		if (op.kind == SqlKind.CAST) {
//			TypeInfo castType = HiveParserTypeConverter.convert(dt);
//
//			if (castType.equals(TypeInfoFactory.byteTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("tinyint");
//			} else if (castType instanceof CharTypeInfo) {
//				castUDF = handleCastForParameterizedType(castType, FunctionRegistry.getFunctionInfo("char"));
//			} else if (castType instanceof VarcharTypeInfo) {
//				castUDF = handleCastForParameterizedType(castType,
//						FunctionRegistry.getFunctionInfo("varchar"));
//			} else if (castType.equals(TypeInfoFactory.stringTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("string");
//			} else if (castType.equals(TypeInfoFactory.booleanTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("boolean");
//			} else if (castType.equals(TypeInfoFactory.shortTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("smallint");
//			} else if (castType.equals(TypeInfoFactory.intTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("int");
//			} else if (castType.equals(TypeInfoFactory.longTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("bigint");
//			} else if (castType.equals(TypeInfoFactory.floatTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("float");
//			} else if (castType.equals(TypeInfoFactory.doubleTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("double");
//			} else if (castType.equals(TypeInfoFactory.timestampTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("timestamp");
//			} else if (castType.equals(TypeInfoFactory.dateTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("date");
//			} else if (castType instanceof DecimalTypeInfo) {
//				castUDF = handleCastForParameterizedType(castType,
//						FunctionRegistry.getFunctionInfo("decimal"));
//			} else if (castType.equals(TypeInfoFactory.binaryTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo("binary");
//			} else if (castType.equals(TypeInfoFactory.intervalDayTimeTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
//			} else if (castType.equals(TypeInfoFactory.intervalYearMonthTypeInfo)) {
//				castUDF = FunctionRegistry.getFunctionInfo(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
//			} else {
//				throw new IllegalStateException("Unexpected type : " + castType.getQualifiedName());
//			}
//		}
//
//		return castUDF;
//	}

//	private static FunctionInfo handleCastForParameterizedType(TypeInfo ti, FunctionInfo fi) {
//		SettableUDF udf = (SettableUDF) fi.getGenericUDF();
//		try {
//			udf.setTypeInfo(ti);
//		} catch (UDFArgumentException e) {
//			throw new RuntimeException(e);
//		}
//		if (fi.isPersistent()) {
//			return new FunctionInfo(fi.getDisplayName(), udf.getClass().getName(), fi.getResources());
//		} else if (fi.isBuiltIn()) {
//			return new FunctionInfo(true, fi.getDisplayName(), (GenericUDF) udf, fi.getResources());
//		} else {
//			return new FunctionInfo(false, fi.getDisplayName(), (GenericUDF) udf, fi.getResources());
//		}
//	}

	// TODO: 1) handle Agg Func Name translation 2) is it correct to add func
	// args as child of func?
	public static ASTNode buildAST(SqlOperator op, List<ASTNode> children) {
		HiveToken hToken = CALCITE_TO_HIVE_TOKEN.get(op);
		ASTNode node;
		if (hToken != null) {
			switch (op.kind) {
				case IN:
				case BETWEEN:
				case ROW:
				case IS_NOT_NULL:
				case IS_NULL:
				case CASE:
				case EXTRACT:
				case FLOOR:
				case OTHER_FUNCTION:
					node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTION, "TOK_FUNCTION");
					node.addChild((ASTNode) HiveASTParseDriver.ADAPTOR.create(hToken.type, hToken.text));
					break;
				default:
					node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(hToken.type, hToken.text);
			}
		} else {
			node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTION, "TOK_FUNCTION");
			if (op.kind != SqlKind.CAST) {
				if (op.kind == SqlKind.MINUS_PREFIX) {
					node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.MINUS, "MINUS");
				} else if (op.kind == SqlKind.PLUS_PREFIX) {
					node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.PLUS, "PLUS");
				} else {
					// Handle COUNT/SUM/AVG function for the case of COUNT(*) and COUNT(DISTINCT)
					if (op instanceof HiveParserSqlCountAggFunction ||
							op instanceof HiveParserSqlSumAggFunction ||
							(op instanceof CalciteUDAF && op.getName().equalsIgnoreCase(SqlStdOperatorTable.AVG.getName()))) {
						if (children.size() == 0) {
							node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTIONSTAR,
									"TOK_FUNCTIONSTAR");
						} else {
							CanAggregateDistinct distinctFunction = (CanAggregateDistinct) op;
							if (distinctFunction.isDistinct()) {
								node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTIONDI,
										"TOK_FUNCTIONDI");
							}
						}
					}
					node.addChild((ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, op.getName()));
				}
			}
		}

		for (ASTNode c : children) {
			HiveASTParseDriver.ADAPTOR.addChild(node, c);
		}
		return node;
	}

	/**
	 * Build AST for flattened Associative expressions ('and', 'or'). Flattened
	 * expressions is of the form or[x,y,z] which is originally represented as
	 * "or[x, or[y, z]]".
	 */
	public static ASTNode buildAST(SqlOperator op, List<ASTNode> children, int i) {
		if (i + 1 < children.size()) {
			HiveToken hToken = CALCITE_TO_HIVE_TOKEN.get(op);
			ASTNode curNode = ((ASTNode) HiveASTParseDriver.ADAPTOR.create(hToken.type, hToken.text));
			HiveASTParseDriver.ADAPTOR.addChild(curNode, children.get(i));
			HiveASTParseDriver.ADAPTOR.addChild(curNode, buildAST(op, children, i + 1));
			return curNode;
		} else {
			return children.get(i);
		}

	}

	// TODO: this is not valid. Function names for built-in UDFs are specified in
	// FunctionRegistry, and only happen to match annotations. For user UDFs, the
	// name is what user specifies at creation time (annotation can be absent,
	// different, or duplicate some other function).
	private static String getName(GenericUDF hiveUDF) {
		String udfName = null;
		if (hiveUDF instanceof GenericUDFBridge) {
			udfName = ((GenericUDFBridge) hiveUDF).getUdfName();
		} else {
			Class<? extends GenericUDF> udfClass = hiveUDF.getClass();
			Annotation udfAnnotation = udfClass.getAnnotation(Description.class);

			if (udfAnnotation != null && udfAnnotation instanceof Description) {
				Description udfDescription = (Description) udfAnnotation;
				udfName = udfDescription.name();
				if (udfName != null) {
					String[] aliases = udfName.split(",");
					if (aliases.length > 0) {
						udfName = aliases[0];
					}
				}
			}

			if (udfName == null || udfName.isEmpty()) {
				udfName = hiveUDF.getClass().getName();
				int indx = udfName.lastIndexOf(".");
				if (indx >= 0) {
					indx += 1;
					udfName = udfName.substring(indx);
				}
			}
		}

		return udfName;
	}

	/**
	 * This class is used to build immutable hashmaps in the static block above.
	 */
	private static class StaticBlockBuilder {
		final Map<String, SqlOperator> hiveToCalcite = new HashMap<>();
		final Map<SqlOperator, HiveToken> calciteToHiveToken = new HashMap<>();
		final Map<SqlOperator, String> reverseOperatorMap = new HashMap<>();

		StaticBlockBuilder() {
			registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveASTParser.PLUS, "+"));
			registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveASTParser.MINUS, "-"));
			registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveASTParser.STAR, "*"));
			registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveASTParser.DIVIDE, "/"));
			registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveASTParser.Identifier, "%"));
			registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveASTParser.KW_AND, "and"));
			registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveASTParser.KW_OR, "or"));
			registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveASTParser.EQUAL, "="));
			registerDuplicateFunction("==", SqlStdOperatorTable.EQUALS, hToken(HiveASTParser.EQUAL, "="));
			registerFunction("<", SqlStdOperatorTable.LESS_THAN, hToken(HiveASTParser.LESSTHAN, "<"));
			registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
					hToken(HiveASTParser.LESSTHANOREQUALTO, "<="));
			registerFunction(">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveASTParser.GREATERTHAN, ">"));
			registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
					hToken(HiveASTParser.GREATERTHANOREQUALTO, ">="));
			registerFunction("not", SqlStdOperatorTable.NOT, hToken(HiveASTParser.KW_NOT, "not"));
			registerDuplicateFunction("!", SqlStdOperatorTable.NOT, hToken(HiveASTParser.KW_NOT, "not"));
			registerFunction("<>", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveASTParser.NOTEQUAL, "<>"));
			registerDuplicateFunction("!=", SqlStdOperatorTable.NOT_EQUALS, hToken(HiveASTParser.NOTEQUAL, "<>"));
			registerFunction("in", HiveParserIN.INSTANCE, hToken(HiveASTParser.Identifier, "in"));
			registerFunction("between", HiveBetween.INSTANCE, hToken(HiveASTParser.Identifier, "between"));
			registerFunction("struct", SqlStdOperatorTable.ROW, hToken(HiveASTParser.Identifier, "struct"));
			registerFunction("isnotnull", SqlStdOperatorTable.IS_NOT_NULL, hToken(HiveASTParser.TOK_ISNOTNULL, "TOK_ISNOTNULL"));
			registerFunction("isnull", SqlStdOperatorTable.IS_NULL, hToken(HiveASTParser.TOK_ISNULL, "TOK_ISNULL"));
			registerFunction("when", SqlStdOperatorTable.CASE, hToken(HiveASTParser.Identifier, "when"));
			registerDuplicateFunction("case", SqlStdOperatorTable.CASE, hToken(HiveASTParser.Identifier, "when"));
			// timebased
			registerFunction("year", HiveParserExtractDate.YEAR,
					hToken(HiveASTParser.Identifier, "year"));
			registerFunction("quarter", HiveParserExtractDate.QUARTER,
					hToken(HiveASTParser.Identifier, "quarter"));
			registerFunction("month", HiveParserExtractDate.MONTH,
					hToken(HiveASTParser.Identifier, "month"));
			registerFunction("weekofyear", HiveParserExtractDate.WEEK,
					hToken(HiveASTParser.Identifier, "weekofyear"));
			registerFunction("day", HiveParserExtractDate.DAY,
					hToken(HiveASTParser.Identifier, "day"));
			registerFunction("hour", HiveParserExtractDate.HOUR,
					hToken(HiveASTParser.Identifier, "hour"));
			registerFunction("minute", HiveParserExtractDate.MINUTE,
					hToken(HiveASTParser.Identifier, "minute"));
			registerFunction("second", HiveParserExtractDate.SECOND,
					hToken(HiveASTParser.Identifier, "second"));
			registerFunction("floor_year", HiveParserFloorDate.YEAR,
					hToken(HiveASTParser.Identifier, "floor_year"));
			registerFunction("floor_quarter", HiveParserFloorDate.QUARTER,
					hToken(HiveASTParser.Identifier, "floor_quarter"));
			registerFunction("floor_month", HiveParserFloorDate.MONTH,
					hToken(HiveASTParser.Identifier, "floor_month"));
			registerFunction("floor_week", HiveParserFloorDate.WEEK,
					hToken(HiveASTParser.Identifier, "floor_week"));
			registerFunction("floor_day", HiveParserFloorDate.DAY,
					hToken(HiveASTParser.Identifier, "floor_day"));
			registerFunction("floor_hour", HiveParserFloorDate.HOUR,
					hToken(HiveASTParser.Identifier, "floor_hour"));
			registerFunction("floor_minute", HiveParserFloorDate.MINUTE,
					hToken(HiveASTParser.Identifier, "floor_minute"));
			registerFunction("floor_second", HiveParserFloorDate.SECOND,
					hToken(HiveASTParser.Identifier, "floor_second"));
		}

		private void registerFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
			reverseOperatorMap.put(calciteFn, name);
			FunctionInfo hFn;
			try {
				hFn = FunctionRegistry.getFunctionInfo(name);
			} catch (SemanticException e) {
				LOG.warn("Failed to load udf " + name, e);
				hFn = null;
			}
			if (hFn != null) {
				String hFnName = getName(hFn.getGenericUDF());
				hiveToCalcite.put(hFnName, calciteFn);

				if (hiveToken != null) {
					calciteToHiveToken.put(calciteFn, hiveToken);
				}
			}
		}

		private void registerDuplicateFunction(String name, SqlOperator calciteFn, HiveToken hiveToken) {
			hiveToCalcite.put(name, calciteFn);
			if (hiveToken != null) {
				calciteToHiveToken.put(calciteFn, hiveToken);
			}
		}
	}

	private static HiveToken hToken(int type, String text) {
		return new HiveToken(type, text);
	}

	/**
	 * UDAF is assumed to be deterministic.
	 */
	public static class CalciteUDAF extends SqlAggFunction implements CanAggregateDistinct {
		private final boolean isDistinct;

		public CalciteUDAF(boolean isDistinct, String opName, SqlReturnTypeInference returnTypeInference,
				SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
			super(opName, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference,
					operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION);
			this.isDistinct = isDistinct;
		}

		@Override
		public boolean isDistinct() {
			return isDistinct;
		}
	}

	/**
	 * CalciteSqlFn.
	 */
	public static class CalciteSqlFn extends SqlFunction {
		private final boolean deterministic;

		public CalciteSqlFn(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
				SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
				SqlFunctionCategory category, boolean deterministic) {
			super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
			this.deterministic = deterministic;
		}

		@Override
		public boolean isDeterministic() {
			return deterministic;
		}
	}

	private static class CalciteUDFInfo {
		private String udfName;
		private SqlReturnTypeInference returnTypeInference;
		private SqlOperandTypeInference operandTypeInference;
		private SqlOperandTypeChecker operandTypeChecker;
	}

	private static CalciteUDFInfo getUDFInfo(String hiveUdfName,
			List<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
		CalciteUDFInfo udfInfo = new CalciteUDFInfo();
		udfInfo.udfName = hiveUdfName;
		udfInfo.returnTypeInference = ReturnTypes.explicit(calciteRetType);
		udfInfo.operandTypeInference = InferTypes.explicit(calciteArgTypes);
		List<SqlTypeFamily> typeFamily = new ArrayList<>();
		for (RelDataType at : calciteArgTypes) {
			typeFamily.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
		}
		udfInfo.operandTypeChecker = OperandTypes.family(Collections.unmodifiableList(typeFamily));
		return udfInfo;
	}

	public static SqlOperator getCalciteFn(String hiveUdfName,
			List<RelDataType> calciteArgTypes, RelDataType calciteRetType, boolean deterministic)
			throws CalciteSemanticException {

		if (hiveUdfName != null && hiveUdfName.trim().equals("<=>")) {
			// We can create Calcite IS_DISTINCT_FROM operator for this. But since our
			// join reordering algo cant handle this anyway there is no advantage of
			// this.So, bail out for now.
			throw new CalciteSemanticException("<=> is not yet supported for cbo.", CalciteSemanticException.UnsupportedFeature.Less_than_equal_greater_than);
		}
		SqlOperator calciteOp;
		CalciteUDFInfo uInf = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);
		switch (hiveUdfName) {
			// Follow hive's rules for type inference as oppose to Calcite's
			// for return type.
			//TODO: Perhaps we should do this for all functions, not just +,-
			case "-":
				calciteOp = new SqlMonotonicBinaryOperator("-", SqlKind.MINUS, 40, true,
						uInf.returnTypeInference, uInf.operandTypeInference, OperandTypes.MINUS_OPERATOR);
				break;
			case "+":
				calciteOp = new SqlMonotonicBinaryOperator("+", SqlKind.PLUS, 40, true,
						uInf.returnTypeInference, uInf.operandTypeInference, OperandTypes.PLUS_OPERATOR);
				break;
			default:
				calciteOp = HIVE_TO_CALCITE.get(hiveUdfName);
				if (null == calciteOp) {
					calciteOp = new CalciteSqlFn(uInf.udfName, SqlKind.OTHER_FUNCTION, uInf.returnTypeInference,
							uInf.operandTypeInference, uInf.operandTypeChecker,
							SqlFunctionCategory.USER_DEFINED_FUNCTION, deterministic);
				}
				break;
		}
		return calciteOp;
	}

	public static SqlAggFunction getCalciteAggFn(String hiveUdfName, boolean isDistinct,
			List<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
		SqlAggFunction calciteAggFn = (SqlAggFunction) HIVE_TO_CALCITE.get(hiveUdfName);

		if (calciteAggFn == null) {
			CalciteUDFInfo udfInfo = getUDFInfo(hiveUdfName, calciteArgTypes, calciteRetType);

			switch (hiveUdfName.toLowerCase()) {
				case "sum":
					calciteAggFn = new HiveParserSqlSumAggFunction(
							isDistinct,
							udfInfo.returnTypeInference,
							udfInfo.operandTypeInference,
							udfInfo.operandTypeChecker);
					break;
				case "count":
					calciteAggFn = new HiveParserSqlCountAggFunction(
							isDistinct,
							udfInfo.returnTypeInference,
							udfInfo.operandTypeInference,
							udfInfo.operandTypeChecker);
					break;
				case "min":
					calciteAggFn = new HiveSqlMinMaxAggFunction(
							udfInfo.returnTypeInference,
							udfInfo.operandTypeInference,
							udfInfo.operandTypeChecker, true);
					break;
				case "max":
					calciteAggFn = new HiveSqlMinMaxAggFunction(
							udfInfo.returnTypeInference,
							udfInfo.operandTypeInference,
							udfInfo.operandTypeChecker, false);
					break;
				default:
					calciteAggFn = new CalciteUDAF(
							isDistinct,
							udfInfo.udfName,
							udfInfo.returnTypeInference,
							udfInfo.operandTypeInference,
							udfInfo.operandTypeChecker);
					break;
			}

		}
		return calciteAggFn;
	}

	static class HiveToken {
		int type;
		String text;
		String[] args;

		HiveToken(int type, String text, String... args) {
			this.type = type;
			this.text = text;
			this.args = args;
		}
	}

	/**
	 * CanAggregateDistinct.
	 */
	public interface CanAggregateDistinct {
		boolean isDistinct();
	}
}
