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
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

	public static NlsString asUnicodeString(String text) {
		return new NlsString(text, ConversionUtil.NATIVE_UTF16_CHARSET_NAME, SqlCollation.IMPLICIT);
	}

	// Overrides CalcitePlanner::canHandleQbForCbo to support SORT BY, CLUSTER BY, etc.
	public static String canHandleQbForCbo(QueryProperties queryProperties) {
		if (!queryProperties.hasPTF() && !queryProperties.usesScript() && !queryProperties.hasLateralViews()) {
			return null;
		}
		String msg = "";
		if (queryProperties.hasPTF()) {
			msg += "has PTF; ";
		}
		if (queryProperties.usesScript()) {
			msg += "uses scripts; ";
		}
		if (queryProperties.hasLateralViews()) {
			msg += "has lateral views; ";
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
				res = TypeConverter.convert(typeInfo, relTypeFactory);
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
}
