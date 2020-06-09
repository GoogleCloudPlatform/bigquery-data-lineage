// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.solutions.datalineage.converter;

import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import com.google.zetasql.resolvedast.ResolvedColumn;

/**
 * A parser to convert ZetaSql's {@link ResolvedColumn} to a {@Link ColumnEntity} by mapping the
 * table name and column name.
 */
public final class ResolvedColumnToColumnEntityConverter {

  /**
   * Maps the Tablename and column name of the ZetaSql parsed column. It sets the entity type as
   * {@code QUERY_LEVEL_TABLE} if the resolved table name is {@code "$"}.
   *
   * @param resolvedColumn the ZetaSql parsed Column information.
   * @return equivalent ColumnEntity information.
   */
  public static ColumnEntity convertToColumnEntity(ResolvedColumn resolvedColumn) {
    return
        ColumnEntity.newBuilder()
            .setTable(identifyDataEntity(resolvedColumn))
            .setColumn(resolvedColumn.getName())
            .build();
  }

  private static DataEntity identifyDataEntity(ResolvedColumn resolvedColumn) {
    if (resolvedColumn.getTableName().startsWith("$")) {
      return DataEntity.newBuilder()
          .setKind(DataEntityTypes.QUERY_LEVEL_TABLE)
          .setSqlResource(resolvedColumn.getTableName())
          .build();
    } else {
      return BigQueryTableCreator
          .usingBestEffort(resolvedColumn.getTableName()).dataEntity();
    }
  }

  private ResolvedColumnToColumnEntityConverter() {
  }
}
