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

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveASTParser;
import org.apache.hadoop.hive.ql.parse.HiveParserRowResolver;
import org.apache.hadoop.hive.ql.parse.HiveParserTypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.HiveParserTypeCheckProcFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.parse.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;

/**
 * Counterpart of hive's JoinCondTypeCheckProcFactory.
 */
public class HiveParserJoinCondTypeCheckProcFactory extends HiveParserTypeCheckProcFactory {

	public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr, HiveParserTypeCheckCtx tcCtx)
			throws SemanticException {
		return HiveParserTypeCheckProcFactory.genExprNode(expr, tcCtx, new HiveParserJoinCondTypeCheckProcFactory());
	}

	/**
	 * Processor for table columns.
	 */
	public static class JoinCondColumnExprProcessor extends HiveParserTypeCheckProcFactory.ColumnExprProcessor {

		@Override
		public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
				Object... nodeOutputs) throws SemanticException {

			HiveParserJoinTypeCheckCtx ctx = (HiveParserJoinTypeCheckCtx) procCtx;
			if (ctx.getError() != null) {
				return null;
			}

			ASTNode expr = (ASTNode) nd;
			ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2) : null;

			if (expr.getType() != HiveASTParser.TOK_TABLE_OR_COL) {
				ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
				return null;
			}

			assert (expr.getChildCount() == 1);
			String tableOrCol = unescapeIdentifier(expr.getChild(0).getText());

			boolean qualifiedAccess = (parent != null && parent.getType() == HiveASTParser.DOT);

			ColumnInfo colInfo = null;
			if (!qualifiedAccess) {
				colInfo = getColInfo(ctx, null, tableOrCol, expr);
				// It's a column.
				return new ExprNodeColumnDesc(colInfo);
			} else if (hasTableAlias(ctx, tableOrCol, expr)) {
				return null;
			} else {
				// Qualified column access for which table was not found
				throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(expr));
			}
		}

		private static boolean hasTableAlias(HiveParserJoinTypeCheckCtx ctx, String tabName, ASTNode expr)
				throws SemanticException {
			int tblAliasCnt = 0;
			for (HiveParserRowResolver rr : ctx.getInputRRList()) {
				if (rr.hasTableAlias(tabName)) {
					tblAliasCnt++;
				}
			}

			if (tblAliasCnt > 1) {
				throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_OR_COLUMN.getMsg(expr));
			}

			return (tblAliasCnt == 1) ? true : false;
		}

		private static ColumnInfo getColInfo(HiveParserJoinTypeCheckCtx ctx, String tabName, String colAlias,
				ASTNode expr) throws SemanticException {
			ColumnInfo tmp;
			ColumnInfo cInfoToRet = null;

			for (HiveParserRowResolver rr : ctx.getInputRRList()) {
				tmp = rr.get(tabName, colAlias);
				if (tmp != null) {
					if (cInfoToRet != null) {
						throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_OR_COLUMN.getMsg(expr));
					}
					cInfoToRet = tmp;
				}
			}

			return cInfoToRet;
		}
	}

	/**
	 * Factory method to get ColumnExprProcessor.
	 *
	 * @return ColumnExprProcessor.
	 */
	@Override
	public HiveParserTypeCheckProcFactory.ColumnExprProcessor getColumnExprProcessor() {
		return new HiveParserJoinCondTypeCheckProcFactory.JoinCondColumnExprProcessor();
	}

	/**
	 * The default processor for typechecking.
	 */
	public static class JoinCondDefaultExprProcessor extends HiveParserTypeCheckProcFactory.DefaultExprProcessor {
		@Override
		protected List<String> getReferenceableColumnAliases(HiveParserTypeCheckCtx ctx) {
			HiveParserJoinTypeCheckCtx jCtx = (HiveParserJoinTypeCheckCtx) ctx;
			List<String> possibleColumnNames = new ArrayList<String>();
			for (HiveParserRowResolver rr : jCtx.getInputRRList()) {
				possibleColumnNames.addAll(rr.getReferenceableColumnAliases(null, -1));
			}

			return possibleColumnNames;
		}

		@Override
		protected ExprNodeColumnDesc processQualifiedColRef(HiveParserTypeCheckCtx ctx, ASTNode expr,
				Object... nodeOutputs) throws SemanticException {
			String tableAlias = unescapeIdentifier(expr.getChild(0).getChild(0)
					.getText());
			// NOTE: tableAlias must be a valid non-ambiguous table alias,
			// because we've checked that in TOK_TABLE_OR_COL's process method.
			ColumnInfo colInfo = getColInfo((HiveParserJoinTypeCheckCtx) ctx, tableAlias,
					((ExprNodeConstantDesc) nodeOutputs[1]).getValue().toString(), expr);

			if (colInfo == null) {
				ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
				return null;
			}
			return new ExprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName(), tableAlias,
					colInfo.getIsVirtualCol());
		}

		private static ColumnInfo getColInfo(HiveParserJoinTypeCheckCtx ctx, String tabName, String colAlias,
				ASTNode expr) throws SemanticException {
			ColumnInfo tmp;
			ColumnInfo cInfoToRet = null;

			for (HiveParserRowResolver rr : ctx.getInputRRList()) {
				tmp = rr.get(tabName, colAlias);
				if (tmp != null) {
					if (cInfoToRet != null) {
						throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_OR_COLUMN.getMsg(expr));
					}
					cInfoToRet = tmp;
				}
			}

			return cInfoToRet;
		}
	}

	/**
	 * Factory method to get DefaultExprProcessor.
	 *
	 * @return DefaultExprProcessor.
	 */
	@Override
	public HiveParserTypeCheckProcFactory.DefaultExprProcessor getDefaultExprProcessor() {
		return new HiveParserJoinCondTypeCheckProcFactory.JoinCondDefaultExprProcessor();
	}
}
