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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * ParquetTableScan.
 */
public class ParquetTableScan extends TableScan implements ParquetRel {

  private final ParquetTable parquetTable;
  private final RelDataType projectRowType;

  public ParquetTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, ParquetTable parquetTable, RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.parquetTable = parquetTable;
    this.projectRowType = projectRowType;
    assert getConvention() == ParquetRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new ParquetTableScan(getCluster(), traitSet, table, parquetTable, projectRowType);
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(new ParquetToEnumerableRelConverterRule());
    planner.addRule(new ParquetProjectRule());
    planner.addRule(new ParquetFilterRule());
  }

  @Override public void implement(ParquetRel.Implementor implementor) {
    implementor.parquetTable = this.parquetTable;
    implementor.table = table;
  }
}

// End ParquetTableScan.java
