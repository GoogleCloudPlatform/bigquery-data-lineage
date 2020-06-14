/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.datalineage.extractor;

import static com.google.cloud.solutions.datalineage.converter.ResolvedColumnToColumnEntityConverter.convertToColumnEntity;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.util.HashMap;

/**
 * Extracts Column lineages for QueryColumns for simple SQL functions like CONCAT etc.
 */
public final class FunctionExpressionsExtractor extends
    ColumnLineageExtractor {

  public FunctionExpressionsExtractor(ResolvedNodes.ResolvedStatement resolvedStatement) {
    super(resolvedStatement);
  }

  @Override
  public String getSupportedColumnType() {
    return "$query";
  }

  @Override
  public ImmutableMap<ColumnEntity, ColumnLineage> extract() {
    HashMap<ColumnEntity, ColumnLineage> exprLineageMapBuilder = new HashMap<>();

    resolvedStatement.accept(
        new ResolvedNodes.Visitor() {
          @Override
          public void visit(ResolvedNodes.ResolvedProjectScan node) {
            node.getExprList()
                .forEach(
                    expr -> {
                      ColumnEntity outputColumn = convertToColumnEntity(expr.getColumn());

                      ImmutableList.Builder<ColumnEntity> exprSourceColumnBuilder =
                          ImmutableList.builder();
                      ImmutableList.Builder<String> exprFunctionsBuilder = ImmutableList.builder();

                      expr.accept(
                          new ResolvedNodes.Visitor() {
                            @Override
                            public void visit(ResolvedNodes.ResolvedFunctionCall functionCall) {
                              exprFunctionsBuilder.add(
                                  String.format(
                                      "%s (%s)",
                                      functionCall.getFunction().getSqlName(),
                                      functionCall.getFunction().getFullName()));

                              functionCall.accept(
                                  new ResolvedNodes.Visitor() {
                                    @Override
                                    public void visit(
                                        ResolvedNodes.ResolvedColumnRef sourceColumn) {
                                      exprSourceColumnBuilder
                                          .add(convertToColumnEntity(sourceColumn.getColumn()));
                                    }
                                  });
                              super.visit(functionCall);
                            }
                          });

                      exprLineageMapBuilder.put(
                          outputColumn,
                          ColumnLineage.newBuilder()
                              .setTarget(outputColumn)
                              .addAllParents(
                                  exprSourceColumnBuilder.build().stream()
                                      .distinct()
                                      .collect(toImmutableSet()))
                              .addAllOperations(exprFunctionsBuilder.build())
                              .build());
                    });
            super.visit(node);
          }
        });

    return ImmutableMap.copyOf(exprLineageMapBuilder);
  }
}
