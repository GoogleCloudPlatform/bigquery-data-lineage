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

import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.fromBigQueryResource;
import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.fromLegacyTableName;
import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.fromLinkedResource;
import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.fromSqlResource;
import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.usingBestEffort;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BigQueryTableCreatorTest {

  @Test
  public void fromLegacyTableName_projectIdWithHyphens_valid() {
    assertThat(fromLegacyTableName("bq-lineage-demo:audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromLegacyTableName_projectIdWithColon_valid() {
    assertThat(fromLegacyTableName("example.com:myproject:audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.com:myproject",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromLegacyTableName_projectIdWithColonAndHyphen_valid() {
    assertThat(fromLegacyTableName("example.co.in:test-project:audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromLegacyTableName_projectIfWithNumber_valid() {
    assertThat(
        fromLegacyTableName("example.co.in:test-project-2:audit_dataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project-2",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromLegacyTableName_TableWithUppercaseStarting_valid() {
    assertThat(fromLegacyTableName("example.co.in:test-project:audit_dataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromLegacyTableName_DatasetWithUppercaseStarting_valid() {
    assertThat(fromLegacyTableName("example.co.in:test-project:AuditDataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "AuditDataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromLegacyTableName_TableWithUnderscore_valid() {
    assertThat(fromLegacyTableName("example.co.in:test-project:audit_dataset.lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage_table"));
  }

  @Test
  public void fromLegacyTableName_projectIdWithUnderscore_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromLegacyTableName("example.co.in:test_project:audit_dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (example.co.in:test_project:audit_dataset.lineage) not in correct format");
  }

  @Test
  public void fromLegacyTableName_datasetWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromLegacyTableName("test-project:audit-dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat()
        .startsWith("input (test-project:audit-dataset.lineage) not in correct format");
  }

  @Test
  public void fromLegacyTableName_tableWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromLegacyTableName("test-project:audit_dataset.lineage-table"));

    assertThat(illegalArgumentException).hasMessageThat()
        .startsWith("input (test-project:audit_dataset.lineage-table) not in correct format");
  }

  // SQL Resource format

  @Test
  public void fromSqlResource_doesNotStartWithBigQueryLiteral_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromSqlResource("example.co.in:test_project.audit_dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (example.co.in:test_project.audit_dataset.lineage) not in correct format");
  }

  @Test
  public void fromSqlResource_projectIdWithHyphens_valid() {
    assertThat(fromSqlResource("bigquery.table.bq-lineage-demo.audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromSqlResource_projectIdWithColon_valid() {
    assertThat(fromSqlResource("bigquery.table.example.com:myproject.audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.com:myproject",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromSqlResource_projectIdWithColonAndHyphen_valid() {
    assertThat(
        fromSqlResource(
            "bigquery.table.example.co.in:test-project.audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromSqlResource_TableWithUnderscore_valid() {
    assertThat(
        fromSqlResource(
            "bigquery.table.example.co.in:test-project.audit_dataset.lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage_table"));
  }

  @Test
  public void fromSqlResource_projectIdWithUnderscore_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                fromSqlResource(
                    "bigquery.table.example.co.in:test_project.audit_dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (bigquery.table.example.co.in:test_project.audit_dataset.lineage) not in correct format");
  }

  @Test
  public void fromSqlResource_datasetWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromSqlResource("bigquery.table.test-project.audit-dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (bigquery.table.test-project.audit-dataset.lineage) not in correct format");
  }

  @Test
  public void fromSqlResource_tableWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromSqlResource("bigquery.table.test-project.audit_dataset.lineage-table"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (bigquery.table.test-project.audit_dataset.lineage-table) not in correct format");
  }

  @Test
  public void fromSqlResource_projectIfWithNumber_valid() {
    assertThat(fromSqlResource(
        "bigquery.table.example.co.in:test-project-2.audit_dataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project-2",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromSqlResource_TableWithUppercaseStarting_valid() {
    assertThat(fromSqlResource(
        "bigquery.table.example.co.in:test-project.audit_dataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromSqlResource_DatasetWithUppercaseStarting_valid() {
    assertThat(fromSqlResource(
        "bigquery.table.example.co.in:test-project.AuditDataset.Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "AuditDataset",
                /*table=*/ "Lineage_table"));
  }

  // BigQuery Resource format
  @Test
  public void fromBqResource_projectIfWithNumber_valid() {
    assertThat(fromBigQueryResource(
        "projects/example.co.in:test-project-2/datasets/audit_dataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project-2",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromBqResource_TableWithUppercaseStarting_valid() {
    assertThat(fromBigQueryResource(
        "projects/example.co.in:test-project/datasets/audit_dataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromBqResource_DatasetWithUppercaseStarting_valid() {
    assertThat(fromBigQueryResource(
        "projects/example.co.in:test-project/datasets/AuditDataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "AuditDataset",
                /*table=*/ "Lineage_table"));
  }


  @Test
  public void fromBqResource_doesNotStartWithProjectsLiteral_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromBigQueryResource("example.co.in:test_project.audit_dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (example.co.in:test_project.audit_dataset.lineage) not in correct format");
  }

  @Test
  public void fromBqResource_projectIdWithHyphens_valid() {
    assertThat(
        fromBigQueryResource("projects/bq-lineage-demo/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromBqResource_projectIdWithColon_valid() {
    assertThat(
        fromBigQueryResource(
            "projects/example.com:myproject/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.com:myproject",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromBqResource_projectIdWithColonAndHyphen_valid() {
    assertThat(
        fromBigQueryResource(
            "projects/example.co.in:test-project/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromBqResource_TableWithUnderscore_valid() {
    assertThat(
        fromBigQueryResource(
            "projects/example.co.in:test-project/datasets/audit_dataset/tables/lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage_table"));
  }

  @Test
  public void fromBqResource_projectIdWithUnderscore_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                fromBigQueryResource(
                    "projects/example.co.in:test_project/datasets/audit_dataset/tables/lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (projects/example.co.in:test_project/datasets/audit_dataset/tables/lineage) not in correct format");
  }

  @Test
  public void fromBqResource_datasetWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromBigQueryResource(
                "projects/test-project/datasets/audit-dataset/tables/lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (projects/test-project/datasets/audit-dataset/tables/lineage) not in correct format");
  }

  @Test
  public void fromBqResource_tableWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                fromBigQueryResource(
                    "projects/test-project/datasets/audit_dataset/tables/lineage-table"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (projects/test-project/datasets/audit_dataset/tables/lineage-table) not in correct format");
  }

  @Test
  public void fromBqResource_tableWithPartitionDecorator_valid() {
    assertThat(
        fromBigQueryResource(
            "projects/example.co.in:test-project/datasets/audit_dataset/tables/lineage_table$20210606"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage_table"));
  }

  // BigQuery Linked Resource format
  @Test
  public void fromLinkedResource_projectIdWithNumber_valid() {
    assertThat(fromLinkedResource(
        "//bigquery.googleapis.com/projects/example.co.in:test-project-2/datasets/audit_dataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project-2",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromLinkedResource_TableWithUppercaseStarting_valid() {
    assertThat(fromLinkedResource(
        "//bigquery.googleapis.com/projects/example.co.in:test-project/datasets/audit_dataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "Lineage_table"));
  }

  @Test
  public void fromLinkedResource_DatasetWithUppercaseStarting_valid() {
    assertThat(fromLinkedResource(
        "//bigquery.googleapis.com/projects/example.co.in:test-project/datasets/AuditDataset/tables/Lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "AuditDataset",
                /*table=*/ "Lineage_table"));
  }


  @Test
  public void fromLinkedResource_doesNotStartWithProjectsLiteral_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromLinkedResource("example.co.in:test_project.audit_dataset.lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (example.co.in:test_project.audit_dataset.lineage) not in correct format");
  }

  @Test
  public void fromLinkedResource_projectIdWithHyphens_valid() {
    assertThat(
        fromLinkedResource(
            "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromLinkedResource_projectIdWithColon_valid() {
    assertThat(
        fromLinkedResource(
            "//bigquery.googleapis.com/projects/google.com:bigquery-lineage-demo/datasets/MyDataSet/tables/MyView"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "google.com:bigquery-lineage-demo",
                /*dataset=*/ "MyDataSet",
                /*table=*/ "MyView"));
  }

  @Test
  public void fromLinkedResource_projectIdWithColonAndHyphen_valid() {
    assertThat(
        fromLinkedResource(
            "//bigquery.googleapis.com/projects/example.co.in:test-project/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void fromLinkedResource_TableWithUnderscore_valid() {
    assertThat(
        fromLinkedResource(
            "//bigquery.googleapis.com/projects/example.co.in:test-project/datasets/audit_dataset/tables/lineage_table"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "example.co.in:test-project",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage_table"));
  }

  @Test
  public void fromLinkedResource_projectIdWithUnderscore_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                fromLinkedResource(
                    "//bigquery.googleapis.com/projects/example.co.in:test_project/datasets/audit_dataset/tables/lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (//bigquery.googleapis.com/projects/example.co.in:test_project/datasets/audit_dataset/tables/lineage) not in correct format");
  }

  @Test
  public void fromLinkedResource_datasetWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromLinkedResource(
                "//bigquery.googleapis.com/projects/test-project/datasets/audit-dataset/tables/lineage"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (//bigquery.googleapis.com/projects/test-project/datasets/audit-dataset/tables/lineage) not in correct format");
  }

  @Test
  public void fromLinkedResource_tableWithHyphen_throwsException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                fromLinkedResource(
                    "//bigquery.googleapis.com/projects/test-project/datasets/audit_dataset/tables/lineage-table"));

    assertThat(illegalArgumentException).hasMessageThat().startsWith(
        "input (//bigquery.googleapis.com/projects/test-project/datasets/audit_dataset/tables/lineage-table) not in correct format");
  }

  @Test
  public void usingBestEffort_legacySqlName_valid() {
    assertThat(usingBestEffort("bq-lineage-demo:audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void usingBestEffort_standardSqlName_valid() {
    assertThat(usingBestEffort("bq-lineage-demo.audit_dataset.lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void usingBestEffort_bqResource_valid() {
    assertThat(usingBestEffort("projects/bq-lineage-demo/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void usingBestEffort_linkedResource_valid() {
    assertThat(usingBestEffort(
        "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/audit_dataset/tables/lineage"))
        .isEqualTo(
            BigQueryTableEntity.create(
                /*projectId=*/ "bq-lineage-demo",
                /*dataset=*/ "audit_dataset",
                /*table=*/ "lineage"));
  }

  @Test
  public void usingBestEffort_unknownFormat_throwsIllegealArgumentException() {
    IllegalArgumentException aex =
        assertThrows(IllegalArgumentException.class,
            () -> usingBestEffort(
                "//xyz.googleapis.com/projects/bq-lineage-demo/datasets/audit_dataset/tables/lineage"));

    assertThat(aex).hasMessageThat().startsWith("");
  }
}
