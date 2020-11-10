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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalHiveDistribution
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSort

/**
 * Rule that matches [[FlinkLogicalHiveDistribution]] which represents
 * Hive's SORT BY, DISTRIBUTE BY, and CLUSTER BY semantics.
 */
class BatchExecHiveDistributionRule extends ConverterRule(
  classOf[FlinkLogicalHiveDistribution],
  FlinkConventions.LOGICAL,
  FlinkConventions.BATCH_PHYSICAL,
  "BatchExecHiveDistributionRule") {

  override def convert(rel: RelNode): RelNode = {
    val hiveDistribution = rel.asInstanceOf[FlinkLogicalHiveDistribution]

    val input = hiveDistribution.getInput
    val requiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val providedTraitSet = hiveDistribution.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    val newInput = RelOptRule.convert(input, requiredTraitSet)
    if (hiveDistribution.collation.getFieldCollations.isEmpty) {
      newInput
      //      val rexBuilder = newInput.getCluster.getRexBuilder
      //      val inputRowType = newInput.getRowType
      //      val inputRefs = List.range(0, inputRowType.getFieldCount)
      //        .map(i => rexBuilder.makeInputRef(inputRowType, i))
      //      val rexProgram = RexProgram.create(
      //        newInput.getRowType,
      //        inputRefs.asJava,
      //        rexBuilder.makeLiteral(true),
      //        inputRowType,
      //        rexBuilder
      //      )
      //      new BatchExecCalc(
      //        newInput.getCluster,
      //        providedTraitSet,
      //        newInput,
      //        rexProgram,
      //        inputRowType
      //      )
    } else {
      new BatchExecSort(
        hiveDistribution.getCluster,
        providedTraitSet,
        newInput,
        hiveDistribution.collation)
    }
  }

}

object BatchExecHiveDistributionRule {
  val INSTANCE: RelOptRule = new BatchExecHiveDistributionRule
}
