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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.planner.delegation.hive.HiveParserContext;

import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Counterpart of hive's SubQueryUtils.
 */
public class HiveParserSubQueryUtils {
	static void extractConjuncts(ASTNode node, List<ASTNode> conjuncts) {
		if (node.getType() != HiveASTParser.KW_AND) {
			conjuncts.add(node);
			return;
		}
		extractConjuncts((ASTNode) node.getChild(0), conjuncts);
		extractConjuncts((ASTNode) node.getChild(1), conjuncts);
	}

	static ASTNode constructTrueCond() {
		ASTNode eq = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.EQUAL, "=");
		ASTNode lhs = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Number, "1");
		ASTNode rhs = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Number, "1");
		HiveASTParseDriver.ADAPTOR.addChild(eq, lhs);
		HiveASTParseDriver.ADAPTOR.addChild(eq, rhs);
		return eq;
	}

	static ASTNode orAST(ASTNode left, ASTNode right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		} else {
			Object o = HiveASTParseDriver.ADAPTOR.create(HiveASTParser.KW_OR, "OR");
			HiveASTParseDriver.ADAPTOR.addChild(o, left);
			HiveASTParseDriver.ADAPTOR.addChild(o, right);
			return (ASTNode) o;
		}
	}

	static ASTNode isNull(ASTNode expr) {
		ASTNode node = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTION, "TOK_FUNCTION");
		node.addChild((ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_ISNULL, "TOK_ISNULL"));
		node.addChild(expr);
		return node;
	}

	static List<ASTNode> findSubQueries(ASTNode node) {
		List<ASTNode> subQueries = new ArrayList<>();
		findSubQueries(node, subQueries);
		return subQueries;
	}

	private static void findSubQueries(ASTNode node, List<ASTNode> subQueries) {
		Deque<ASTNode> stack = new ArrayDeque<>();
		stack.push(node);

		while (!stack.isEmpty()) {
			ASTNode next = stack.pop();

			switch (next.getType()) {
				case HiveASTParser.TOK_SUBQUERY_EXPR:
					subQueries.add(next);
					break;
				default:
					int childCount = next.getChildCount();
					for (int i = childCount - 1; i >= 0; i--) {
						stack.push((ASTNode) next.getChild(i));
					}
			}
		}
	}

	static HiveParserQBSubQuery buildSubQuery(String outerQueryId,
			int sqIdx,
			ASTNode sqAST,
			ASTNode originalSQAST,
			HiveParserContext ctx,
			FrameworkConfig frameworkConfig,
			RelOptCluster cluster) throws SemanticException {
		ASTNode sqOp = (ASTNode) sqAST.getChild(0);
		ASTNode sq = (ASTNode) sqAST.getChild(1);
		ASTNode outerQueryExpr = (ASTNode) sqAST.getChild(2);

		/*
		 * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
		 */
		if (outerQueryExpr != null && outerQueryExpr.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {

			throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
					originalSQAST.getChild(1), "Only 1 SubQuery expression is supported."));
		}

		return new HiveParserQBSubQuery(outerQueryId, sqIdx, sq, outerQueryExpr,
				buildSQOperator(sqOp),
				originalSQAST,
				ctx,
				frameworkConfig,
				cluster);
	}

	static HiveParserQBSubQuery.SubQueryTypeDef buildSQOperator(ASTNode astSQOp) throws SemanticException {
		ASTNode opAST = (ASTNode) astSQOp.getChild(0);
		HiveParserQBSubQuery.SubQueryType type = HiveParserQBSubQuery.SubQueryType.get(opAST);
		return new HiveParserQBSubQuery.SubQueryTypeDef(opAST, type);
	}

	/*
	 * is this expr a UDAF invocation; does it imply windowing
	 * @return
	 * 0 if implies neither
	 * 1 if implies aggregation
	 * 2 if implies count
	 * 3 if implies windowing
	 */
	static int checkAggOrWindowing(ASTNode expressionTree) throws SemanticException {
		int exprTokenType = expressionTree.getToken().getType();
		if (exprTokenType == HiveASTParser.TOK_FUNCTION
				|| exprTokenType == HiveASTParser.TOK_FUNCTIONDI
				|| exprTokenType == HiveASTParser.TOK_FUNCTIONSTAR) {
			assert (expressionTree.getChildCount() != 0);
			if (expressionTree.getChild(expressionTree.getChildCount() - 1).getType()
					== HiveASTParser.TOK_WINDOWSPEC) {
				return 3;
			}
			if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
				String functionName = HiveParserBaseSemanticAnalyzer.unescapeIdentifier(expressionTree.getChild(0)
						.getText());
				GenericUDAFResolver udafResolver = FunctionRegistry.getGenericUDAFResolver(functionName);
				if (udafResolver != null) {
					// we need to know if it is COUNT since this is specialized for IN/NOT IN
					// corr subqueries.
					if (udafResolver instanceof GenericUDAFCount) {
						return 2;
					}
					return 1;
				}
			}
		}
		int r = 0;
		for (int i = 0; i < expressionTree.getChildCount(); i++) {
			int c = checkAggOrWindowing((ASTNode) expressionTree.getChild(i));
			r = Math.max(r, c);
		}
		return r;
	}

	static ASTNode subQueryWhere(ASTNode insertClause) {
		if (insertClause.getChildCount() > 2 &&
				insertClause.getChild(2).getType() == HiveASTParser.TOK_WHERE) {
			return (ASTNode) insertClause.getChild(2);
		}
		return null;
	}

	static ASTNode createColRefAST(String tabAlias, String colName) {
		ASTNode dot = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.DOT, ".");
		ASTNode tabAst = createTabRefAST(tabAlias);
		ASTNode colAst = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, colName);
		dot.addChild(tabAst);
		dot.addChild(colAst);
		return dot;
	}

	static ASTNode createAliasAST(String colName) {
		return (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, colName);
	}

	static ASTNode createTabRefAST(String tabAlias) {
		ASTNode tabAst = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
		ASTNode tabName = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, tabAlias);
		tabAst.addChild(tabName);
		return tabAst;
	}

	/*
	 * Set of functions to create the Null Check Query for Not-In SubQuery predicates.
	 * For a SubQuery predicate like:
	 *   a not in (select b from R2 where R2.y > 5)
	 * The Not In null check query is:
	 *   (select count(*) as c from R2 where R2.y > 5 and b is null)
	 * This Subquery is joined with the Outer Query plan on the join condition 'c = 0'.
	 * The join condition ensures that in case there are null values in the joining column
	 * the Query returns no rows.
	 *
	 * The AST tree for this is:
	 *
	 * ^(TOK_QUERY
	 *    ^(TOK FROM
	 *        ^(TOK_SUBQUERY
	 *            {the input SubQuery, with correlation removed}
	 *            subQueryAlias
	 *          )
	 *     )
	 *     ^(TOK_INSERT
	 *         ^(TOK_DESTINATION...)
	 *         ^(TOK_SELECT
	 *             ^(TOK_SELECTEXPR {ast tree for count *}
	 *          )
	 *          ^(TOK_WHERE
	 *             {is null check for joining column}
	 *           )
	 *      )
	 * )
	 */
	static ASTNode buildNotInNullCheckQuery(ASTNode subQueryAST,
			String subQueryAlias,
			String cntAlias,
			List<ASTNode> corrExprs,
			HiveParserRowResolver sqRR) {

		subQueryAST = (ASTNode) HiveASTParseDriver.ADAPTOR.dupTree(subQueryAST);
		ASTNode qry = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_QUERY, "TOK_QUERY");

		qry.addChild(buildNotInNullCheckFrom(subQueryAST, subQueryAlias));
		ASTNode insertAST = buildNotInNullCheckInsert();
		qry.addChild(insertAST);
		insertAST.addChild(buildNotInNullCheckSelect(cntAlias));
		insertAST.addChild(buildNotInNullCheckWhere(subQueryAST,
				subQueryAlias, corrExprs, sqRR));

		return qry;
	}

	/*
	 * build:
	 *    ^(TOK FROM
	 *        ^(TOK_SUBQUERY
	 *            {the input SubQuery, with correlation removed}
	 *            subQueryAlias
	 *          )
	 *     )

	 */
	static ASTNode buildNotInNullCheckFrom(ASTNode subQueryAST, String subQueryAlias) {
		ASTNode from = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FROM, "TOK_FROM");
		ASTNode sqExpr = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_SUBQUERY, "TOK_SUBQUERY");
		sqExpr.addChild(subQueryAST);
		sqExpr.addChild(createAliasAST(subQueryAlias));
		from.addChild(sqExpr);
		return from;
	}

	/*
	 * build
	 *     ^(TOK_INSERT
	 *         ^(TOK_DESTINATION...)
	 *      )
	 */
	static ASTNode buildNotInNullCheckInsert() {
		ASTNode insert = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_INSERT, "TOK_INSERT");
		ASTNode dest = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_DESTINATION, "TOK_DESTINATION");
		ASTNode dir = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_DIR, "TOK_DIR");
		ASTNode tfile = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_TMP_FILE, "TOK_TMP_FILE");
		insert.addChild(dest);
		dest.addChild(dir);
		dir.addChild(tfile);

		return insert;
	}

	/*
	 * build:
	 *         ^(TOK_SELECT
	 *             ^(TOK_SELECTEXPR {ast tree for count *}
	 *          )
	 */
	static ASTNode buildNotInNullCheckSelect(String cntAlias) {
		ASTNode select = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_SELECT, "TOK_SELECT");
		ASTNode selectExpr = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_SELEXPR, "TOK_SELEXPR");
		ASTNode countStar = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_FUNCTIONSTAR, "TOK_FUNCTIONSTAR");
		ASTNode alias = (createAliasAST(cntAlias));

		countStar.addChild((ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, "count"));
		select.addChild(selectExpr);
		selectExpr.addChild(countStar);
		selectExpr.addChild(alias);

		return select;
	}

	/*
	 * build:
	 *          ^(TOK_WHERE
	 *             {is null check for joining column}
	 *           )
	 */
	static ASTNode buildNotInNullCheckWhere(ASTNode subQueryAST,
			String sqAlias,
			List<ASTNode> corrExprs,
			HiveParserRowResolver sqRR) {

		ASTNode sqSelect = (ASTNode) subQueryAST.getChild(1).getChild(1);
		ASTNode selExpr = (ASTNode) sqSelect.getChild(0);
		String colAlias = null;

		if (selExpr.getChildCount() == 2) {
			colAlias = selExpr.getChild(1).getText();
		} else if (selExpr.getChild(0).getType() != HiveASTParser.TOK_ALLCOLREF) {
			colAlias = sqAlias + "_ninc_col0";
			selExpr.addChild((ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Identifier, colAlias));
		} else {
			List<ColumnInfo> signature = sqRR.getRowSchema().getSignature();
			ColumnInfo joinColumn = signature.get(0);
			String[] joinColName = sqRR.reverseLookup(joinColumn.getInternalName());
			colAlias = joinColName[1];
		}

		ASTNode searchCond = isNull(createColRefAST(sqAlias, colAlias));

		for (ASTNode e : corrExprs) {
			ASTNode p = (ASTNode) HiveASTParseDriver.ADAPTOR.dupTree(e);
			p = isNull(p);
			searchCond = orAST(searchCond, p);
		}

		ASTNode where = (ASTNode) HiveASTParseDriver.ADAPTOR.create(HiveASTParser.TOK_WHERE, "TOK_WHERE");
		where.addChild(searchCond);
		return where;
	}

	static ASTNode buildNotInNullJoinCond(String subqueryAlias, String cntAlias) {

		ASTNode eq = (ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.EQUAL, "=");

		eq.addChild(createColRefAST(subqueryAlias, cntAlias));
		eq.addChild((ASTNode)
				HiveASTParseDriver.ADAPTOR.create(HiveASTParser.Number, "0"));

		return eq;
	}

	static void checkForSubqueries(ASTNode node) throws SemanticException {
		// allow NOT but throw an error for rest
		if (node.getType() == HiveASTParser.TOK_SUBQUERY_EXPR
				&& node.getParent().getType() != HiveASTParser.KW_NOT) {
			throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
					"Invalid subquery. Subquery in SELECT could only be top-level expression"));
		}
		for (int i = 0; i < node.getChildCount(); i++) {
			checkForSubqueries((ASTNode) node.getChild(i));
		}
	}

	/*
	 * Given a TOK_SELECT this checks IF there is a subquery
	 *  it is top level expression, else it throws an error
	 */
	public static void checkForTopLevelSubqueries(ASTNode selExprList) throws SemanticException {
		// should be either SELECT or SELECT DISTINCT
		assert (selExprList.getType() == HiveASTParser.TOK_SELECT
				|| selExprList.getType() == HiveASTParser.TOK_SELECTDI);
		for (int i = 0; i < selExprList.getChildCount(); i++) {
			ASTNode selExpr = (ASTNode) selExprList.getChild(i);
			// could get either query hint or select expr
			assert (selExpr.getType() == HiveASTParser.TOK_SELEXPR
					|| selExpr.getType() == HiveASTParser.QUERY_HINT);

			if (selExpr.getType() == HiveASTParser.QUERY_HINT) {
				// skip query hints
				continue;
			}

			if (selExpr.getChildCount() == 1
					&& selExpr.getChild(0).getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
				if (selExprList.getType() == HiveASTParser.TOK_SELECTDI) {
					throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
							"Invalid subquery. Subquery with DISTINCT clause is not supported!"));

				}
				continue; //we are good since subquery is top level expression
			}
			// otherwise we need to make sure that there is no subquery at any level
			for (int j = 0; j < selExpr.getChildCount(); j++) {
				checkForSubqueries((ASTNode) selExpr.getChild(j));
			}
		}
	}

	/**
	 * ISubQueryJoinInfo.
	 */
	public interface ISubQueryJoinInfo {
		String getAlias();

		HiveParserQBSubQuery getSubQuery();
	}

	;


	/*
	 * Using CommonTreeAdaptor because the Adaptor in ParseDriver doesn't carry
	 * the token indexes when duplicating a Tree.
	 */
	static final CommonTreeAdaptor ADAPTOR = new CommonTreeAdaptor();
}
