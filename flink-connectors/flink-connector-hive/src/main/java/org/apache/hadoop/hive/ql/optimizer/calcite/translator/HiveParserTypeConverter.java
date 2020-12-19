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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveParserSqlFunctionConverter.HiveToken;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;
import org.apache.hadoop.hive.ql.parse.HiveParserRowResolver;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Counterpart of hive's TypeConverter.
 */
public class HiveParserTypeConverter {

	private static final Map<String, HiveToken> calciteToHiveTypeNameMap;

	// TODO: Handling of char[], varchar[], string...
	static {
		Map<String, HiveToken> map = new HashMap<>();
		map.put(SqlTypeName.BOOLEAN.getName(), new HiveToken(HiveASTParser.TOK_BOOLEAN, "TOK_BOOLEAN"));
		map.put(SqlTypeName.TINYINT.getName(), new HiveToken(HiveASTParser.TOK_TINYINT, "TOK_TINYINT"));
		map.put(SqlTypeName.SMALLINT.getName(), new HiveToken(HiveASTParser.TOK_SMALLINT, "TOK_SMALLINT"));
		map.put(SqlTypeName.INTEGER.getName(), new HiveToken(HiveASTParser.TOK_INT, "TOK_INT"));
		map.put(SqlTypeName.BIGINT.getName(), new HiveToken(HiveASTParser.TOK_BIGINT, "TOK_BIGINT"));
		map.put(SqlTypeName.FLOAT.getName(), new HiveToken(HiveASTParser.TOK_FLOAT, "TOK_FLOAT"));
		map.put(SqlTypeName.DOUBLE.getName(), new HiveToken(HiveASTParser.TOK_DOUBLE, "TOK_DOUBLE"));
		map.put(SqlTypeName.DATE.getName(), new HiveToken(HiveASTParser.TOK_DATE, "TOK_DATE"));
		map.put(SqlTypeName.TIMESTAMP.getName(), new HiveToken(HiveASTParser.TOK_TIMESTAMP, "TOK_TIMESTAMP"));
		map.put(SqlTypeName.INTERVAL_YEAR.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_MONTH.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_YEAR_MONTH.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_DAY.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_DAY_HOUR.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_DAY_MINUTE.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_DAY_SECOND.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_HOUR.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_HOUR_MINUTE.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_HOUR_SECOND.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_MINUTE.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_MINUTE_SECOND.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.INTERVAL_SECOND.getName(),
				new HiveToken(HiveASTParser.Identifier, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
		map.put(SqlTypeName.BINARY.getName(), new HiveToken(HiveASTParser.TOK_BINARY, "TOK_BINARY"));
		calciteToHiveTypeNameMap = Collections.unmodifiableMap(map);
	}

	/**
	 * Convert Hive Types To Calcite Types.
	 */
	public static RelDataType getType(RelOptCluster cluster,
			List<ColumnInfo> cInfoLst) throws CalciteSemanticException {
		RexBuilder rexBuilder = cluster.getRexBuilder();
		RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
		List<RelDataType> fieldTypes = new LinkedList<RelDataType>();
		List<String> fieldNames = new LinkedList<String>();

		for (ColumnInfo ci : cInfoLst) {
			fieldTypes.add(convert(ci.getType(), dtFactory));
			fieldNames.add(ci.getInternalName());
		}
		return dtFactory.createStructType(fieldTypes, fieldNames);
	}

	public static RelDataType getType(RelOptCluster cluster, HiveParserRowResolver rr, List<String> neededCols) throws CalciteSemanticException {
		RexBuilder rexBuilder = cluster.getRexBuilder();
		RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
		RowSchema rs = rr.getRowSchema();
		List<RelDataType> fieldTypes = new LinkedList<>();
		List<String> fieldNames = new LinkedList<>();

		for (ColumnInfo ci : rs.getSignature()) {
			if (neededCols == null || neededCols.contains(ci.getInternalName())) {
				fieldTypes.add(convert(ci.getType(), dtFactory));
				fieldNames.add(ci.getInternalName());
			}
		}
		return dtFactory.createStructType(fieldTypes, fieldNames);
	}

	public static RelDataType convert(TypeInfo type, RelDataTypeFactory dtFactory)
			throws CalciteSemanticException {
		RelDataType convertedType = null;

		switch (type.getCategory()) {
			case PRIMITIVE:
				convertedType = convert((PrimitiveTypeInfo) type, dtFactory);
				break;
			case LIST:
				convertedType = convert((ListTypeInfo) type, dtFactory);
				break;
			case MAP:
				convertedType = convert((MapTypeInfo) type, dtFactory);
				break;
			case STRUCT:
				convertedType = convert((StructTypeInfo) type, dtFactory);
				break;
			case UNION:
				convertedType = convert((UnionTypeInfo) type, dtFactory);
				break;
		}
		return convertedType;
	}

	public static RelDataType convert(PrimitiveTypeInfo type, RelDataTypeFactory dtFactory) {
		RelDataType convertedType = null;

		switch (type.getPrimitiveCategory()) {
			case VOID:
				convertedType = dtFactory.createSqlType(SqlTypeName.NULL);
				break;
			case BOOLEAN:
				convertedType = dtFactory.createSqlType(SqlTypeName.BOOLEAN);
				break;
			case BYTE:
				convertedType = dtFactory.createSqlType(SqlTypeName.TINYINT);
				break;
			case SHORT:
				convertedType = dtFactory.createSqlType(SqlTypeName.SMALLINT);
				break;
			case INT:
				convertedType = dtFactory.createSqlType(SqlTypeName.INTEGER);
				break;
			case LONG:
				convertedType = dtFactory.createSqlType(SqlTypeName.BIGINT);
				break;
			case FLOAT:
				convertedType = dtFactory.createSqlType(SqlTypeName.FLOAT);
				break;
			case DOUBLE:
				convertedType = dtFactory.createSqlType(SqlTypeName.DOUBLE);
				break;
			case STRING:
				convertedType = dtFactory.createTypeWithCharsetAndCollation(
						dtFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
						Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
				break;
			case DATE:
				convertedType = dtFactory.createSqlType(SqlTypeName.DATE);
				break;
			case TIMESTAMP:
				convertedType = dtFactory.createSqlType(SqlTypeName.TIMESTAMP);
				break;
			case INTERVAL_YEAR_MONTH:
				convertedType = dtFactory.createSqlIntervalType(
						new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
				break;
			case INTERVAL_DAY_TIME:
				convertedType = dtFactory.createSqlIntervalType(
						new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, new SqlParserPos(1, 1)));
				break;
			case BINARY:
				convertedType = dtFactory.createSqlType(SqlTypeName.BINARY);
				break;
			case DECIMAL:
				DecimalTypeInfo dtInf = (DecimalTypeInfo) type;
				convertedType = dtFactory
						.createSqlType(SqlTypeName.DECIMAL, dtInf.precision(), dtInf.scale());
				break;
			case VARCHAR:
				convertedType = dtFactory.createTypeWithCharsetAndCollation(
						dtFactory.createSqlType(SqlTypeName.VARCHAR, ((BaseCharTypeInfo) type).getLength()),
						Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
				break;
			case CHAR:
				convertedType = dtFactory.createTypeWithCharsetAndCollation(
						dtFactory.createSqlType(SqlTypeName.CHAR, ((BaseCharTypeInfo) type).getLength()),
						Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
				break;
			case UNKNOWN:
				convertedType = dtFactory.createSqlType(SqlTypeName.OTHER);
				break;
		}

		if (null == convertedType) {
			throw new RuntimeException("Unsupported Type : " + type.getTypeName());
		}

		return dtFactory.createTypeWithNullability(convertedType, true);
	}

	public static RelDataType convert(ListTypeInfo lstType,
			RelDataTypeFactory dtFactory) throws CalciteSemanticException {
		RelDataType elemType = convert(lstType.getListElementTypeInfo(), dtFactory);
		return dtFactory.createArrayType(elemType, -1);
	}

	public static RelDataType convert(MapTypeInfo mapType, RelDataTypeFactory dtFactory)
			throws CalciteSemanticException {
		RelDataType keyType = convert(mapType.getMapKeyTypeInfo(), dtFactory);
		RelDataType valueType = convert(mapType.getMapValueTypeInfo(), dtFactory);
		return dtFactory.createMapType(keyType, valueType);
	}

	public static RelDataType convert(StructTypeInfo structType,
			final RelDataTypeFactory dtFactory) throws CalciteSemanticException {
		List<RelDataType> fTypes = new ArrayList<RelDataType>(structType.getAllStructFieldTypeInfos().size());
		for (TypeInfo ti : structType.getAllStructFieldTypeInfos()) {
			fTypes.add(convert(ti, dtFactory));
		}
		return dtFactory.createStructType(fTypes, structType.getAllStructFieldNames());
	}

	public static RelDataType convert(UnionTypeInfo unionType, RelDataTypeFactory dtFactory)
			throws CalciteSemanticException {
		// Union type is not supported in Calcite.
		throw new CalciteSemanticException("Union type is not supported", CalciteSemanticException.UnsupportedFeature.Union_type);
	}

	public static TypeInfo convert(RelDataType rType) {
		if (rType.isStruct()) {
			return convertStructType(rType);
		} else if (rType.getComponentType() != null) {
			return convertListType(rType);
		} else if (rType.getKeyType() != null) {
			return convertMapType(rType);
		} else {
			return convertPrimitiveType(rType);
		}
	}

	public static TypeInfo convertStructType(RelDataType rType) {
		List<TypeInfo> fTypes = rType.getFieldList().stream().map(f -> convert(f.getType())).collect(Collectors.toList());
		List<String> fNames = rType.getFieldList().stream().map(RelDataTypeField::getName).collect(Collectors.toList());
		return TypeInfoFactory.getStructTypeInfo(fNames, fTypes);
	}

	public static TypeInfo convertMapType(RelDataType rType) {
		return TypeInfoFactory.getMapTypeInfo(convert(rType.getKeyType()),
				convert(rType.getValueType()));
	}

	public static TypeInfo convertListType(RelDataType rType) {
		return TypeInfoFactory.getListTypeInfo(convert(rType.getComponentType()));
	}

	public static TypeInfo convertPrimitiveType(RelDataType rType) {
		switch (rType.getSqlTypeName()) {
			case BOOLEAN:
				return TypeInfoFactory.booleanTypeInfo;
			case TINYINT:
				return TypeInfoFactory.byteTypeInfo;
			case SMALLINT:
				return TypeInfoFactory.shortTypeInfo;
			case INTEGER:
				return TypeInfoFactory.intTypeInfo;
			case BIGINT:
				return TypeInfoFactory.longTypeInfo;
			case FLOAT:
				return TypeInfoFactory.floatTypeInfo;
			case DOUBLE:
				return TypeInfoFactory.doubleTypeInfo;
			case DATE:
				return TypeInfoFactory.dateTypeInfo;
			case TIMESTAMP:
				return TypeInfoFactory.timestampTypeInfo;
			case INTERVAL_YEAR:
			case INTERVAL_MONTH:
			case INTERVAL_YEAR_MONTH:
				return TypeInfoFactory.intervalYearMonthTypeInfo;
			case INTERVAL_DAY:
			case INTERVAL_DAY_HOUR:
			case INTERVAL_DAY_MINUTE:
			case INTERVAL_DAY_SECOND:
			case INTERVAL_HOUR:
			case INTERVAL_HOUR_MINUTE:
			case INTERVAL_HOUR_SECOND:
			case INTERVAL_MINUTE:
			case INTERVAL_MINUTE_SECOND:
			case INTERVAL_SECOND:
				return TypeInfoFactory.intervalDayTimeTypeInfo;
			case BINARY:
				return TypeInfoFactory.binaryTypeInfo;
			case DECIMAL:
				return TypeInfoFactory.getDecimalTypeInfo(rType.getPrecision(), rType.getScale());
			case VARCHAR:
				int varcharLength = rType.getPrecision();
				if (varcharLength < 1 || varcharLength > HiveVarchar.MAX_VARCHAR_LENGTH) {
					return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
				} else {
					return TypeInfoFactory.getVarcharTypeInfo(varcharLength);
				}
			case CHAR:
				int charLength = rType.getPrecision();
				if (charLength < 1 || charLength > HiveChar.MAX_CHAR_LENGTH) {
					return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
				} else {
					return TypeInfoFactory.getCharTypeInfo(charLength);
				}
			case OTHER:
			default:
				return TypeInfoFactory.voidTypeInfo;
		}

	}

	/**
	 * Convert Calcite Types To Hive Types.
	 */
	public static HiveToken hiveToken(RelDataType calciteType) {
		HiveToken ht = null;

		switch (calciteType.getSqlTypeName()) {
			case CHAR: {
				ht = new HiveToken(HiveASTParser.TOK_CHAR, "TOK_CHAR", String.valueOf(calciteType.getPrecision()));
			}
			break;
			case VARCHAR: {
				if (calciteType.getPrecision() == Integer.MAX_VALUE) {
					ht = new HiveToken(HiveASTParser.TOK_STRING, "TOK_STRING", String.valueOf(calciteType
							.getPrecision()));
				} else {
					ht = new HiveToken(HiveASTParser.TOK_VARCHAR, "TOK_VARCHAR", String.valueOf(calciteType
							.getPrecision()));
				}
			}
			break;
			case DECIMAL: {
				ht = new HiveToken(HiveASTParser.TOK_DECIMAL, "TOK_DECIMAL", String.valueOf(calciteType
						.getPrecision()), String.valueOf(calciteType.getScale()));
			}
			break;
			default:
				ht = calciteToHiveTypeNameMap.get(calciteType.getSqlTypeName().getName());
		}

		return ht;
	}
}
