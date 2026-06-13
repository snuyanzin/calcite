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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PruneEmptyRules}.
 *
 * <p>The plan before and after "optimization" is diffed against a .xml file
 * using {@link DiffRepository}. See {@link RelOptRulesTest} for the procedure
 * to add a new test case.
 */
class PruneEmptyRulesTest extends RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  @Nullable
  private static DiffRepository diffRepos = null;

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    if (diffRepos != null) {
      diffRepos.checkActualAndReferenceFiles();
    }
  }

  @Override RelOptFixture fixture() {
    RelOptFixture fixture = super.fixture()
        .withDiffRepos(DiffRepository.lookup(PruneEmptyRulesTest.class));
    diffRepos = fixture.diffRepos();
    return fixture;
  }

  /**
   * Program that mirrors the rule set and ordering used by downstream HEP
   * pipelines (e.g. Flink): the {@code REDUCE_EXPRESSIONS} rules run first, and
   * the set-op prune rule runs <em>before</em>
   * {@link PruneEmptyRules#PROJECT_INSTANCE} /
   * {@link PruneEmptyRules#FILTER_INSTANCE}.
   *
   * <p>Once {@link org.apache.calcite.rel.metadata.RelMdPredicates} infers
   * constant predicates from a single-row {@code VALUES}
   * (<a href="https://issues.apache.org/jira/browse/CALCITE-6044">[CALCITE-6044]</a>),
   * {@code REDUCE_EXPRESSIONS} leaves a
   * {@link org.apache.calcite.rel.core.Project} over the (empty or single-row)
   * {@code VALUES} instead of a bare empty {@code VALUES}. The set-op prune
   * rule must still recognize the empty input through that {@code Project},
   * otherwise the set operator (and its now constant-folded siblings) is never
   * pruned and the plan blows up.
   */
  private static HepProgram pruneEmptySetOpWithReduceExpressions(RelOptRule setOpInstance) {
    return new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(setOpInstance)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.FILTER_INSTANCE)
        .build();
  }

  /** Tests that {@link PruneEmptyRules#MINUS_INSTANCE} prunes a {@code Minus}
   * whose first (empty) input is hidden behind a {@code Project} that
   * {@code REDUCE_EXPRESSIONS} introduces. The first input is empty, therefore
   * the whole expression is empty. */
  @Test void testEmptyMinusThroughProject() {
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "where x > 30\n"
        + "except\n"
        + "select * from (values (20, 2))\n"
        + "except\n"
        + "select * from (values (40, 4))";
    sql(sql)
        .withProgram(pruneEmptySetOpWithReduceExpressions(PruneEmptyRules.MINUS_INSTANCE))
        .check();
  }

  /** As {@link #testEmptyMinusThroughProject()} but for
   * {@link PruneEmptyRules#UNION_INSTANCE}: the empty branch is removed even
   * though it is wrapped in a {@code Project}. */
  @Test void testEmptyUnionThroughProject() {
    final String sql = "select * from (\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "union all\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30";
    sql(sql)
        .withProgram(pruneEmptySetOpWithReduceExpressions(PruneEmptyRules.UNION_INSTANCE))
        .check();
  }

  /** As {@link #testEmptyMinusThroughProject()} but for
   * {@link PruneEmptyRules#INTERSECT_INSTANCE}: an empty input (behind a
   * {@code Project}) makes the whole {@code Intersect} empty. */
  @Test void testEmptyIntersectThroughProject() {
    final String sql = "select * from (values (30, 3))\n"
        + "intersect\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y) where x > 50\n"
        + "intersect\n"
        + "select * from (values (30, 3))";
    sql(sql)
        .withProgram(pruneEmptySetOpWithReduceExpressions(PruneEmptyRules.INTERSECT_INSTANCE))
        .check();
  }
}
