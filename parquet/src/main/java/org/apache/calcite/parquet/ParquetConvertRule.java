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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * ParquetConvertRule.
 */
abstract class ParquetConvertRule  extends ConverterRule {

  protected final Convention out;

  ParquetConvertRule(Class<? extends RelNode> clazz,
      String description) {
    this(clazz, Predicates.<RelNode>alwaysTrue(), description);
  }

  <R extends RelNode> ParquetConvertRule(Class<R> clazz,
      Predicate<? super R> predicate,
      String description) {
    super(clazz, predicate, Convention.NONE,
        ParquetRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description);
    this.out = ParquetRel.CONVENTION;
  }
}

// End ParquetConvertRule.java
