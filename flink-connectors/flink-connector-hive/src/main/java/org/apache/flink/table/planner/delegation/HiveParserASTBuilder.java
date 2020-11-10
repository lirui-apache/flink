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

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveASTParseDriver;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.escapeSQLString;

/**
 * Counterpart of hive's ASTBuilder.
 */
public class HiveParserASTBuilder {

	public static HiveParserASTBuilder construct(int tokenType, String text) {
		HiveParserASTBuilder b = new HiveParserASTBuilder();
		b.curr = createAST(tokenType, text);
		return b;
	}

	public static ASTNode createAST(int tokenType, String text) {
		return (ASTNode) HiveASTParseDriver.ADAPTOR.create(tokenType, text);
	}

	public static ASTNode destNode() {
		return HiveParserASTBuilder
				.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_DESTINATION, "TOK_DESTINATION")
				.add(
						HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_DIR, "TOK_DIR").add(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_TMP_FILE,
								"TOK_TMP_FILE")).node();
	}

	public static ASTNode join(ASTNode left, ASTNode right, JoinRelType joinType, ASTNode cond,
			boolean semiJoin) {
		HiveParserASTBuilder b = null;

		switch (joinType) {
			case INNER:
				if (semiJoin) {
					b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_LEFTSEMIJOIN, "TOK_LEFTSEMIJOIN");
				} else {
					b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_JOIN, "TOK_JOIN");
				}
				break;
			case LEFT:
				b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_LEFTOUTERJOIN, "TOK_LEFTOUTERJOIN");
				break;
			case RIGHT:
				b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_RIGHTOUTERJOIN, "TOK_RIGHTOUTERJOIN");
				break;
			case FULL:
				b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_FULLOUTERJOIN, "TOK_FULLOUTERJOIN");
				break;
		}

		b.add(left).add(right).add(cond);
		return b.node();
	}

	public static ASTNode subQuery(ASTNode qry, String alias) {
		return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_SUBQUERY, "TOK_SUBQUERY").add(qry)
				.add(org.apache.hadoop.hive.ql.parse.HiveASTParser.Identifier, alias).node();
	}

	public static ASTNode qualifiedName(String tableName, String colName) {
		HiveParserASTBuilder b = HiveParserASTBuilder
				.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.DOT, ".")
				.add(
						HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL").add(
								org.apache.hadoop.hive.ql.parse.HiveASTParser.Identifier, tableName)).add(org.apache.hadoop.hive.ql.parse.HiveASTParser.Identifier, colName);
		return b.node();
	}

	public static ASTNode unqualifiedName(String colName) {
		HiveParserASTBuilder b = HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL").add(
				org.apache.hadoop.hive.ql.parse.HiveASTParser.Identifier, colName);
		return b.node();
	}

	public static ASTNode where(ASTNode cond) {
		return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_WHERE, "TOK_WHERE").add(cond).node();
	}

	public static ASTNode having(ASTNode cond) {
		return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_HAVING, "TOK_HAVING").add(cond).node();
	}

	public static ASTNode limit(Object offset, Object limit) {
		return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_LIMIT, "TOK_LIMIT")
				.add(org.apache.hadoop.hive.ql.parse.HiveASTParser.Number, offset.toString())
				.add(org.apache.hadoop.hive.ql.parse.HiveASTParser.Number, limit.toString()).node();
	}

	public static ASTNode selectExpr(ASTNode expr, String alias) {
		return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_SELEXPR, "TOK_SELEXPR").add(expr)
				.add(org.apache.hadoop.hive.ql.parse.HiveASTParser.Identifier, alias).node();
	}

	public static ASTNode literal(RexLiteral literal) {
		return literal(literal, false);
	}

	public static ASTNode literal(RexLiteral literal, boolean useTypeQualInLiteral) {
		Object val = null;
		int type = 0;
		SqlTypeName sqlType = literal.getType().getSqlTypeName();

		switch (sqlType) {
			case BINARY:
			case DATE:
			case TIME:
			case TIMESTAMP:
			case INTERVAL_DAY:
			case INTERVAL_DAY_HOUR:
			case INTERVAL_DAY_MINUTE:
			case INTERVAL_DAY_SECOND:
			case INTERVAL_HOUR:
			case INTERVAL_HOUR_MINUTE:
			case INTERVAL_HOUR_SECOND:
			case INTERVAL_MINUTE:
			case INTERVAL_MINUTE_SECOND:
			case INTERVAL_MONTH:
			case INTERVAL_SECOND:
			case INTERVAL_YEAR:
			case INTERVAL_YEAR_MONTH:
				if (literal.getValue() == null) {
					return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_NULL, "TOK_NULL").node();
				}
				break;
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case DOUBLE:
			case DECIMAL:
			case FLOAT:
			case REAL:
			case VARCHAR:
			case CHAR:
			case BOOLEAN:
				if (literal.getValue3() == null) {
					return HiveParserASTBuilder.construct(org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_NULL, "TOK_NULL").node();
				}
		}

		switch (sqlType) {
			case TINYINT:
				if (useTypeQualInLiteral) {
					val = literal.getValue3() + "Y";
				} else {
					val = literal.getValue3();
				}
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.IntegralLiteral;
				break;
			case SMALLINT:
				if (useTypeQualInLiteral) {
					val = literal.getValue3() + "S";
				} else {
					val = literal.getValue3();
				}
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.IntegralLiteral;
				break;
			case INTEGER:
				val = literal.getValue3();
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.IntegralLiteral;
				break;
			case BIGINT:
				if (useTypeQualInLiteral) {
					val = literal.getValue3() + "L";
				} else {
					val = literal.getValue3();
				}
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.IntegralLiteral;
				break;
			case DOUBLE:
				val = literal.getValue3() + "D";
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.NumberLiteral;
				break;
			case DECIMAL:
				val = literal.getValue3() + "BD";
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.NumberLiteral;
				break;
			case FLOAT:
			case REAL:
				val = literal.getValue3();
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.Number;
				break;
			case VARCHAR:
			case CHAR:
				val = literal.getValue3();
				String escapedVal = escapeSQLString(String.valueOf(val));
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.StringLiteral;
				val = "'" + escapedVal + "'";
				break;
			case BOOLEAN:
				val = literal.getValue3();
				type = ((Boolean) val).booleanValue() ? org.apache.hadoop.hive.ql.parse.HiveASTParser.KW_TRUE : org.apache.hadoop.hive.ql.parse.HiveASTParser.KW_FALSE;
				break;
			case DATE: {
				val = literal.getValue();
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_DATELITERAL;
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				val = df.format(((Calendar) val).getTime());
				val = "'" + val + "'";
			}
			break;
			case TIME:
			case TIMESTAMP: {
				val = literal.getValue();
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_TIMESTAMPLITERAL;
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				val = df.format(((Calendar) val).getTime());
				val = "'" + val + "'";
			}
			break;
			case INTERVAL_YEAR:
			case INTERVAL_MONTH:
			case INTERVAL_YEAR_MONTH: {
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_INTERVAL_YEAR_MONTH_LITERAL;
				BigDecimal monthsBd = (BigDecimal) literal.getValue();
				HiveIntervalYearMonth intervalYearMonth = new HiveIntervalYearMonth(monthsBd.intValue());
				val = "'" + intervalYearMonth.toString() + "'";
			}
			break;
			case INTERVAL_DAY:
			case INTERVAL_DAY_HOUR:
			case INTERVAL_DAY_MINUTE:
			case INTERVAL_DAY_SECOND:
			case INTERVAL_HOUR:
			case INTERVAL_HOUR_MINUTE:
			case INTERVAL_HOUR_SECOND:
			case INTERVAL_MINUTE:
			case INTERVAL_MINUTE_SECOND:
			case INTERVAL_SECOND: {
				type = org.apache.hadoop.hive.ql.parse.HiveASTParser.TOK_INTERVAL_DAY_TIME_LITERAL;
				BigDecimal millisBd = (BigDecimal) literal.getValue();

				// Calcite literal is in millis, convert to seconds
				BigDecimal secsBd = millisBd.divide(BigDecimal.valueOf(1000));
				HiveIntervalDayTime intervalDayTime = new HiveIntervalDayTime(secsBd);
				val = "'" + intervalDayTime.toString() + "'";
			}
			break;
			case NULL:
				type = HiveASTParser.TOK_NULL;
				break;

			//binary type should not be seen.
			case BINARY:
			default:
				throw new RuntimeException("Unsupported Type: " + sqlType);
		}

		return (ASTNode) HiveASTParseDriver.ADAPTOR.create(type, String.valueOf(val));
	}

	ASTNode curr;

	public ASTNode node() {
		return curr;
	}

	public HiveParserASTBuilder add(int tokenType, String text) {
		HiveASTParseDriver.ADAPTOR.addChild(curr, createAST(tokenType, text));
		return this;
	}

	public HiveParserASTBuilder add(HiveParserASTBuilder b) {
		HiveASTParseDriver.ADAPTOR.addChild(curr, b.curr);
		return this;
	}

	public HiveParserASTBuilder add(ASTNode n) {
		if (n != null) {
			HiveASTParseDriver.ADAPTOR.addChild(curr, n);
		}
		return this;
	}
}
