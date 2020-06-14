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

import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor;

/**
 * Extracts Column lineages for output columns which use Aggregation functions like SUM, AVG etc.
 */
public class SimpleAggregateExtractor extends
    ColumnLineageExtractor {

  public SimpleAggregateExtractor(ResolvedNodes.ResolvedStatement resolvedStatement) {
    super(resolvedStatement);
  }

  @Override
  public String getSupportedColumnType() {
    return "$aggregate";
  }

  @Override
  public ImmutableMap<ColumnEntity, ColumnLineage> extract() {

    ImmutableMap.Builder<ColumnEntity, ColumnLineage> lineageBuilder = ImmutableMap.builder();
    resolvedStatement.accept(
        new Visitor() {
          @Override
          public void visit(ResolvedAggregateScan aggScan) {
            aggScan
                .getAggregateList()
                .forEach(
                    resolvedCol -> {
                      ImmutableSet.Builder<ColumnEntity> sources = ImmutableSet.builder();
                      ImmutableList.Builder<String> aggOperations = ImmutableList.builder();

                      resolvedCol
                          .getExpr()
                          .accept(
                              new Visitor() {
                                @Override
                                public void visit(ResolvedAggregateFunctionCall aggFuncCall) {
                                  aggOperations.add(
                                      String.format(
                                          "%s (%s)",
                                          aggFuncCall.getFunction().getSqlName(),
                                          aggFuncCall.getFunction().getFullName()));

                                  aggFuncCall.accept(
                                      new Visitor() {
                                        @Override
                                        public void visit(ResolvedColumnRef resolvedArguments) {
                                          sources.add(
                                              convertToColumnEntity(resolvedArguments.getColumn()));
                                        }
                                      });
                                }
                              });

                      ColumnEntity col = convertToColumnEntity(resolvedCol.getColumn());

                      lineageBuilder.put(
                          col,
                          ColumnLineage.newBuilder()
                              .setTarget(col)
                              .addAllParents(sources.build())
                              .addAllOperations(aggOperations.build())
                              .build());
                    });
          }
        });

    return lineageBuilder.build();
  }
}
