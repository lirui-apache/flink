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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeSubQueryDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A sub-class of Hive's RexNodeConverter. We need this because we need to call some Calcite APIs with shaded
 * signatures, or create sql operators of a different kind, etc.
 */
public class HiveParserRexNodeConverter extends RexNodeConverter {

	private final RelOptCluster cluster;

	public HiveParserRexNodeConverter(RelOptCluster cluster, RelDataType inpDataType, com.google.common.collect.ImmutableMap<String,
			Integer> outerNameToPosMap, com.google.common.collect.ImmutableMap<String, Integer> nameToPosMap, RowResolver hiveRR,
			RowResolver outerRR, int offset, boolean flattenExpr, int correlatedId) {
		super(cluster, inpDataType, outerNameToPosMap, nameToPosMap, hiveRR, outerRR, offset, flattenExpr, correlatedId);
		this.cluster = cluster;
	}

	public HiveParserRexNodeConverter(RelOptCluster cluster, RelDataType inpDataType,
			com.google.common.collect.ImmutableMap<String, Integer> nameToPosMap, int offset, boolean flattenExpr) {
		super(cluster, inpDataType, nameToPosMap, offset, flattenExpr);
		this.cluster = cluster;
	}

	@Override
	public RexNode convert(ExprNodeDesc expr) throws SemanticException {
		if (expr instanceof ExprNodeSubQueryDesc) {
			return convertSubQuery((ExprNodeSubQueryDesc) expr);
		} else if (expr instanceof ExprNodeGenericFuncDesc) {
			return convertGenericFunc((ExprNodeGenericFuncDesc) expr);
		} else {
			return super.convert(expr);
		}
	}

	private RexNode convertGenericFunc(ExprNodeGenericFuncDesc func) throws SemanticException {
		GenericUDF tgtUdf = func.getGenericUDF();
		if (tgtUdf instanceof GenericUDFIn) {
			List<RexNode> childRexNodes = new ArrayList<>();
			for (ExprNodeDesc childExpr : func.getChildren()) {
				childRexNodes.add(convert(childExpr));
			}
			return cluster.getRexBuilder().makeCall(HiveParserIN.INSTANCE, childRexNodes);
		} else {
			return super.convert(func);
		}
	}

	private RexNode convertSubQuery(final ExprNodeSubQueryDesc subQueryDesc) throws SemanticException {
		if (subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.IN) {
			/*
			 * Check.5.h :: For In and Not In the SubQuery must implicitly or
			 * explicitly only contain one select item.
			 */
			if (subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
				throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
						"SubQuery can contain only 1 item in Select List."));
			}
			//create RexNode for LHS
			RexNode rexNodeLhs = convert(subQueryDesc.getSubQueryLhs());

			//create RexSubQuery node
			return HiveParserUtils.rexSubQueryIn(subQueryDesc.getRexSubQuery(), Collections.singletonList(rexNodeLhs));
		} else if (subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.EXISTS) {
			RexNode subQueryNode = RexSubQuery.exists(subQueryDesc.getRexSubQuery());
			return subQueryNode;
		} else if (subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.SCALAR) {
			if (subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
				throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
						"SubQuery can contain only 1 item in Select List."));
			}
			//create RexSubQuery node
			RexNode rexSubQuery = RexSubQuery.scalar(subQueryDesc.getRexSubQuery());
			return rexSubQuery;
		} else {
			throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
					"Invalid subquery: " + subQueryDesc.getType()));
		}
	}
}
