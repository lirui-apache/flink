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

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static org.apache.flink.table.planner.delegation.hive.parse.HiveParserBaseSemanticAnalyzer.unescapeSQLString;

/**
 * Counterpart of hive's RowFormatParams.
 */
public class HiveParserRowFormatParams {
	String fieldDelim = null;
	String fieldEscape = null;
	String collItemDelim = null;
	String mapKeyDelim = null;
	String lineDelim = null;
	String nullFormat = null;

	public String getFieldDelim() {
		return fieldDelim;
	}

	public String getFieldEscape() {
		return fieldEscape;
	}

	public String getCollItemDelim() {
		return collItemDelim;
	}

	public String getMapKeyDelim() {
		return mapKeyDelim;
	}

	public String getLineDelim() {
		return lineDelim;
	}

	public String getNullFormat() {
		return nullFormat;
	}

	protected void analyzeRowFormat(ASTNode child) throws SemanticException {
		child = (ASTNode) child.getChild(0);
		int numChildRowFormat = child.getChildCount();
		for (int numC = 0; numC < numChildRowFormat; numC++) {
			ASTNode rowChild = (ASTNode) child.getChild(numC);
			switch (rowChild.getToken().getType()) {
				case HiveASTParser.TOK_TABLEROWFORMATFIELD:
					fieldDelim = unescapeSQLString(rowChild.getChild(0)
							.getText());
					if (rowChild.getChildCount() >= 2) {
						fieldEscape = unescapeSQLString(rowChild
								.getChild(1).getText());
					}
					break;
				case HiveASTParser.TOK_TABLEROWFORMATCOLLITEMS:
					collItemDelim = unescapeSQLString(rowChild
							.getChild(0).getText());
					break;
				case HiveASTParser.TOK_TABLEROWFORMATMAPKEYS:
					mapKeyDelim = unescapeSQLString(rowChild.getChild(0)
							.getText());
					break;
				case HiveASTParser.TOK_TABLEROWFORMATLINES:
					lineDelim = unescapeSQLString(rowChild.getChild(0)
							.getText());
					if (!lineDelim.equals("\n")
							&& !lineDelim.equals("10")) {
						throw new SemanticException(SemanticAnalyzer.generateErrorMessage(rowChild,
								ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
					}
					break;
				case HiveASTParser.TOK_TABLEROWFORMATNULL:
					nullFormat = unescapeSQLString(rowChild.getChild(0)
							.getText());
					break;
				default:
					throw new AssertionError("Unkown Token: " + rowChild);
			}
		}
	}
}
