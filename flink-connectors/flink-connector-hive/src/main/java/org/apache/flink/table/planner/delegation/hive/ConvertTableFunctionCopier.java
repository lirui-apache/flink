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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.ArrayList;
import java.util.List;

/**
 * A RexCopier that converts RexCall for HiveTableFunctionScan.
 */
public class ConvertTableFunctionCopier extends ConvertSqlFunctionCopier {

	private final RelOptCluster cluster;
	// LHS RelNode
	private final RelNode leftRel;

	public ConvertTableFunctionCopier(RelOptCluster cluster, RelNode leftRel, SqlOperatorTable opTable, SqlNameMatcher nameMatcher) {
		super(cluster.getRexBuilder(), opTable, nameMatcher);
		this.cluster = cluster;
		this.leftRel = leftRel;
	}

	@Override
	public RexNode visitCall(RexCall call) {
		SqlOperator operator = call.getOperator();
		if (isHiveCalciteSqlFn(operator)) {
			// explicitly use USER_DEFINED_TABLE_FUNCTION since Hive can set USER_DEFINED_FUNCTION for UDTF
			SqlOperator convertedOperator = convertOperator(operator, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
			// create RexCorrelVariable
			CorrelationId correlId = cluster.createCorrel();
			RelDataTypeFactory.Builder dataTypeBuilder = cluster.getTypeFactory().builder();
			dataTypeBuilder.addAll(leftRel.getRowType().getFieldList());
			dataTypeBuilder.addAll(call.getType().getFieldList());
			RexNode correlRex = builder.makeCorrel(dataTypeBuilder.uniquify().build(), correlId);
			// create RexFieldAccess
			List<RexNode> convertedOperands = new ArrayList<>();
			for (RexNode operand : call.getOperands()) {
				if (operand instanceof RexInputRef) {
					convertedOperands.add(builder.makeFieldAccess(correlRex, ((RexInputRef) operand).getIndex()));
				} else if (operand instanceof RexLiteral) {
					convertedOperands.add(operand);
				} else {
					throw new IllegalArgumentException(String.format("RexCall %s has unsupported operand %s", call, operand));
				}
			}
			// create RexCall
			return builder.makeCall(call.getType(), convertedOperator, convertedOperands);
		}
		return super.visitCall(call);
	}
}
