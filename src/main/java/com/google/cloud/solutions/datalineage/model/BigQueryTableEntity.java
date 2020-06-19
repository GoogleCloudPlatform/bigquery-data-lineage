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

package com.google.cloud.solutions.datalineage.model;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import java.io.Serializable;

/**
 * Value class to represent a BiQgQuery Table entity.
 */
@AutoValue
public abstract class BigQueryTableEntity implements DataEntityConvertible, Serializable {

  public abstract String getProjectId();

  public abstract String getDataset();

  public abstract String getTable();

  /**
   * Returns {@code true} if the table is a temporary table.
   * <p> It uses rule dataset name starts with '_' or table name starts with '_' or 'anon'.
   */
  public final boolean isTempTable() {
    return getDataset().startsWith("_")
        || getTable().startsWith("_")
        || getTable().startsWith("anon");
  }

  public static Builder builder() {
    return new AutoValue_BigQueryTableEntity.Builder();
  }

  public static BigQueryTableEntity create(String projectId, String dataset, String table) {
    return builder()
        .setProjectId(projectId)
        .setDataset(dataset)
        .setTable(table)
        .build();
  }

  @Override
  public DataEntity dataEntity() {
    return DataEntity.newBuilder()
        .setKind(DataEntityTypes.BIGQUERY_TABLE)
        .setLinkedResource(
            String.format("//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                getProjectId(),
                getDataset(),
                getTable()))
        .setSqlResource(
            String.format("bigquery.table.%s.%s.%s", getProjectId(), getDataset(), getTable()))
        .build();
  }

  public String getLegacySqlName() {
    return String.format("%s:%s.%s", getProjectId(), getDataset(), getTable());
  }

  public String getStandSqlName() {
    return String.format("%s.%s.%s", getProjectId(), getDataset(), getTable());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDataset(String dataset);

    public abstract Builder setTable(String table);

    public abstract BigQueryTableEntity build();
  }
}
