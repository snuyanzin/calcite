/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.parquet;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * ParquetProject.
 */
public class ParquetProject extends Project implements ParquetRel {

  public ParquetProject(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, input, projects, rowType);
    assert getConvention() == ParquetRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    List<String> fieldNames = getRowType().getFieldNames();
    for (String fieldName : fieldNames) {
      implementor.addProjectedField(fieldName);
    }
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new ParquetProject(getCluster(), traitSet, input, projects, rowType);
  }
}

// End ParquetProject.java
