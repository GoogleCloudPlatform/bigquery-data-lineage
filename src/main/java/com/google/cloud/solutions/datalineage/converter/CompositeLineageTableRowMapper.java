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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * An Adapter to convert {@link CompositeLineage} to a table row for writing into BigQuery.
 */
public final class CompositeLineageTableRowMapper {

  private static final DateTimeFormatter BQ_TIMESTAMP_FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");
  private final CompositeLineage compositeLineage;
  private final JobInformation jobInformation;
  private final TableLineage tableLineage;
  protected final ImmutableList<ColumnLineage> columnsLineage;

  /**
   * Provides a Factory for for building Serializable function for conversion.
   */
  public static SerializableFunction<CompositeLineage, TableRow> newMapper() {
    return (CompositeLineage tableLineage) ->
        new CompositeLineageTableRowMapper(tableLineage)
            .buildCompleteRow();
  }

  @VisibleForTesting
  CompositeLineageTableRowMapper(CompositeLineage compositeLineage) {
    this.compositeLineage = compositeLineage;
    this.jobInformation = compositeLineage.getJobInformation();
    this.tableLineage = compositeLineage.getTableLineage();
    this.columnsLineage = ImmutableList.copyOf(compositeLineage.getColumnsLineageList());
  }

  /**
   * Converts individual lineage entries to a complete BigQuery table row. Look at {@code
   * lineage_bigquery_table_schema.json} to understand the required schema.
   *
   * @return a BigQuery Table row containing all lineage information.
   */
  public TableRow buildCompleteRow() {

    TableRow completeRow = new TableRow()
        .set("reconcileTime", buildTimestampString(compositeLineage.getReconcileTime()))
        .set("jobInformation", buildJobInformation())
        .set("tableLineage", buildTableLineage());

    if (columnsLineage != null && !columnsLineage.isEmpty()) {
      completeRow.set("columnLineage", buildColumnsLineage());
    }

    return completeRow;
  }

  private TableRow buildJobInformation() {
    TableRow jobInfoRow = new TableRow()
        .set("jobTime", buildTimestampString(jobInformation.getJobTime()))
        .set("actuator", jobInformation.getActuator());
    if (jobInformation.getJobId() != null) {
      jobInfoRow.set("jobId", jobInformation.getJobId());
    }
    if (isNotBlank(jobInformation.getJobType())) {
      jobInfoRow.set("jobType", jobInformation.getJobType());
    }

    return jobInfoRow;
  }

  private TableRow buildTableLineage() {
    TableRow tableLineageRow = new TableRow();

    tableLineageRow.set("target", buildDataEntity(tableLineage.getTarget()));

    if (tableLineage.getParentsCount() > 0) {
      tableLineageRow.set("parents", buildDataEntities(tableLineage.getParentsList()));
    }

    if (isNotBlank(tableLineage.getOperation())) {
      tableLineageRow.set("operation", tableLineage.getOperation());
    }

    return tableLineageRow;
  }

  private ImmutableList<TableRow> buildColumnsLineage() {
    return columnsLineage.stream()
        .map(CompositeLineageTableRowMapper::buildColumnLineage)
        .collect(toImmutableList());
  }

  private static TableRow buildColumnLineage(ColumnLineage colLineage) {
    TableRow colLineageRow = new TableRow();

    colLineageRow.set("target", buildColumnEntity(colLineage.getTarget()));

    if (colLineage.getOperationsCount() > 0) {
      colLineageRow.set("operations", colLineage.getOperationsList());
    }

    if (colLineage.getParentsCount() > 0) {
      colLineageRow.set("parents", buildColumnEntities(colLineage.getParentsList()));
    }

    return colLineageRow;
  }

  private static TableRow buildDataEntity(DataEntity dataEntity) {
    return new TableRow()
        .set("kind", dataEntity.getKind().toString())
        .set("sqlResource", dataEntity.getSqlResource())
        .set("linkedResource", dataEntity.getLinkedResource());
  }

  private static TableRow buildColumnEntity(ColumnEntity columnEntity) {
    TableRow columnEntityRow = new TableRow();

    if (!columnEntity.getTable().getKind().equals(DataEntityTypes.UNKNOWN)) {
      columnEntityRow.set("table", buildDataEntity(columnEntity.getTable()));
    }

    return columnEntityRow.set("column", columnEntity.getColumn());
  }

  private static ImmutableList<TableRow> buildDataEntities(Collection<DataEntity> dataEntities) {
    return dataEntities.stream().map(CompositeLineageTableRowMapper::buildDataEntity)
        .collect(toImmutableList());
  }

  private static ImmutableList<TableRow> buildColumnEntities(
      Collection<ColumnEntity> columnEntities) {
    return columnEntities.stream().map(CompositeLineageTableRowMapper::buildColumnEntity)
        .collect(toImmutableList());
  }

  /**
   * Utility function to make a timestamp in RFC-3339 format.
   */
  private static String buildTimestampString(long temporal) {
    return BQ_TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(temporal).atZone(ZoneId.of("UTC")));
  }
}
