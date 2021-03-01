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

import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.
 */
public class HiveParserPTFInvocationSpec {

	PartitionedTableFunctionSpec function;

	public void setFunction(PartitionedTableFunctionSpec function) {
		this.function = function;
	}

	public PartitionedTableFunctionSpec getFunction() {
		return function;
	}

	/**
	 * NullOrder.
	 */
	public enum NullOrder {
		NULLS_FIRST,
		NULLS_LAST;
	}

	/**
	 * OrderSpec.
	 */
	public static class OrderSpec {
		ArrayList<OrderExpression> expressions;

		public OrderSpec() {
		}

		public ArrayList<OrderExpression> getExpressions() {
			return expressions;
		}

		public void addExpression(OrderExpression c) {
			expressions = expressions == null ? new ArrayList<>() : expressions;
			expressions.add(c);
		}

		protected void prefixBy(PartitionSpec pSpec) {
			if (pSpec == null || pSpec.getExpressions() == null) {
				return;
			}
			if (expressions == null) {
				expressions = new ArrayList<>();
			}
			for (int i = pSpec.getExpressions().size() - 1; i >= 0; i--) {
				expressions.add(0, new OrderExpression(pSpec.getExpressions().get(i)));
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((expressions == null) ? 0 : expressions.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			OrderSpec other = (OrderSpec) obj;
			if (expressions == null) {
				return other.expressions == null;
			} else {
				return expressions.equals(other.expressions);
			}
		}

		@Override
		public String toString() {
			return String.format("orderColumns=%s", PTFUtils.toString(expressions));
		}
	}

	/**
	 * OrderExpression.
	 */
	public static class OrderExpression extends PartitionExpression {
		Order order;
		NullOrder nullOrder;

		public OrderExpression() {
			order = Order.ASC;
			nullOrder = NullOrder.NULLS_FIRST;
		}

		public OrderExpression(PartitionExpression peSpec) {
			super(peSpec);
			order = Order.ASC;
			nullOrder = NullOrder.NULLS_FIRST;
		}

		public Order getOrder() {
			return order;
		}

		public void setOrder(Order order) {
			this.order = order;
		}

		public NullOrder getNullOrder() {
			return nullOrder;
		}

		public void setNullOrder(NullOrder nullOrder) {
			this.nullOrder = nullOrder;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + ((order == null) ? 0 : order.hashCode());
			result = prime * result + ((nullOrder == null) ? 0 : nullOrder.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!super.equals(obj)) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			OrderExpression other = (OrderExpression) obj;
			if (order != other.order) {
				return false;
			}
			return nullOrder == other.nullOrder;
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", super.toString(), order, nullOrder);
		}
	}

	/**
	 * PartitioningSpec.
	 */
	public static class PartitioningSpec {
		PartitionSpec partSpec;
		OrderSpec orderSpec;

		public PartitionSpec getPartSpec() {
			return partSpec;
		}

		public void setPartSpec(PartitionSpec partSpec) {
			this.partSpec = partSpec;
		}

		public OrderSpec getOrderSpec() {
			return orderSpec;
		}

		public void setOrderSpec(OrderSpec orderSpec) {
			this.orderSpec = orderSpec;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((orderSpec == null) ? 0 : orderSpec.hashCode());
			result = prime * result + ((partSpec == null) ? 0 : partSpec.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			PartitioningSpec other = (PartitioningSpec) obj;
			if (orderSpec == null) {
				if (other.orderSpec != null) {
					return false;
				}
			} else if (!orderSpec.equals(other.orderSpec)) {
				return false;
			}
			if (partSpec == null) {
				return other.partSpec == null;
			} else {
				return partSpec.equals(other.partSpec);
			}
		}

		@Override
		public String toString() {
			return String.format("PartitioningSpec=[%s%s]",
					partSpec == null ? "" : partSpec,
					orderSpec == null ? "" : orderSpec);
		}
	}

	/**
	 * PartitionedTableFunctionSpec.
	 */
	public static class PartitionedTableFunctionSpec extends PTFInputSpec {
		String name;
		String alias;
		List<ASTNode> args;
		PartitioningSpec partitioning;
		PTFInputSpec input;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getAlias() {
			return alias;
		}

		public void setAlias(String alias) {
			this.alias = alias;
		}

		public List<ASTNode> getArgs() {
			return args;
		}

		public void setArgs(List<ASTNode> args) {
			this.args = args;
		}

		public PartitioningSpec getPartitioning() {
			return partitioning;
		}

		public void setPartitioning(PartitioningSpec partitioning) {
			this.partitioning = partitioning;
		}

		@Override
		public PTFInputSpec getInput() {
			return input;
		}

		public void setInput(PTFInputSpec input) {
			this.input = input;
		}

		public PartitionSpec getPartition() {
			return getPartitioning() == null ? null : getPartitioning().getPartSpec();
		}

		public void setPartition(PartitionSpec partSpec) {
			partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
			partitioning.setPartSpec(partSpec);
		}

		public OrderSpec getOrder() {
			return getPartitioning() == null ? null : getPartitioning().getOrderSpec();
		}

		public void setOrder(OrderSpec orderSpec) {
			partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
			partitioning.setOrderSpec(orderSpec);
		}

		public void addArg(ASTNode arg) {
			args = args == null ? new ArrayList<>() : args;
			args.add(arg);
		}

		@Override
		public String getQueryInputName() {
			return input.getQueryInputName();
		}

		@Override
		public PTFQueryInputSpec getQueryInput() {
			return input.getQueryInput();
		}
	}
}
