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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ParquetToEnumerableConverter.
 */
public class ParquetToEnumerableConverter extends ConverterImpl implements EnumerableRel {

  public ParquetToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, child);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));

    final BlockBuilder list = new BlockBuilder();
    final ParquetRel.Implementor parquetImplementor = new ParquetRel.Implementor();
    parquetImplementor.visitChild(0, getInput());

    final Expression table =
        list.append("table", parquetImplementor.table.getExpression(ParquetTable.class));

    final Expression fields =
        list.append("fields", constantArrayList(parquetImplementor.projectedFields, String.class));

    final Expression predicates =
        list.append("predicates", Expressions.constant(parquetImplementor.getFilter()));

    Expression enumerable =
        list.append("enumerable", Expressions.call(table, "runQuery", fields, predicates));
        //Hook.QUERY_PLAN.run(predicates);
    list.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ParquetToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  private static <T> List<Expression> constantList(List<T> values) {
    return values.stream().map(Expressions::constant).collect(Collectors.toList());
  }
}

// End ParquetToEnumerableConverter.java
