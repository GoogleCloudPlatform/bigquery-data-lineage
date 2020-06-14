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
import com.google.cloud.solutions.datalineage.model.QueryColumns;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;

/**
 * An Extractor to identify output columns of a SQL statement.
 */
public final class OutputColumnExtractor {

  private final ResolvedStatement resolvedStatement;

  public OutputColumnExtractor(ResolvedStatement resolvedStatement) {
    this.resolvedStatement = resolvedStatement;
  }

  /**
   * Returns a map of output columnNames to dependent column information.
   */
  public QueryColumns extract() {
    ImmutableMap.Builder<String, ColumnEntity> outputColumnBuilder = ImmutableMap.builder();
    ImmutableSet.Builder<String> nonTableColumnTypes = ImmutableSet.builder();

    resolvedStatement.accept(
        new ResolvedNodes.Visitor() {
          @Override
          public void visit(ResolvedNodes.ResolvedOutputColumn outputColumn) {
            ResolvedColumn resolvedColumn = outputColumn.getColumn();
            outputColumnBuilder.put(
                outputColumn.getName(),
                convertToColumnEntity(resolvedColumn));

            if (resolvedColumn.getTableName().startsWith("$")) {
              nonTableColumnTypes.add(resolvedColumn.getTableName());
            }

            super.visit(outputColumn);
          }
        });

    return QueryColumns.builder()
        .setColumnMap(outputColumnBuilder.build())
        .build();
  }
}