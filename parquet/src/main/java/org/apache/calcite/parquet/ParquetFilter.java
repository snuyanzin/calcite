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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * ParquetFilter.
 */
public class ParquetFilter extends Filter implements ParquetRel {

  private final String match;

  public ParquetFilter(RelOptCluster cluster,
      RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == ParquetRel.CONVENTION;
    assert getConvention() == child.getConvention();

    this.match = condition.toString();
  }

  @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ParquetFilter(getCluster(), getTraitSet(), input, getCondition());
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.setFilter(match);
  }
}

// End ParquetFilter.java
