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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TransformInformation;
import com.google.cloud.solutions.datalineage.service.BigQueryTableLoadService;
import com.google.cloud.solutions.datalineage.service.BigQueryZetaSqlSchemaLoader;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class QueryJobExtractorTest {

  @Test
  public void extract_ddlCreateStatementNoParents_valid() {

    assertThat(
        new QueryJobExtractor(
            TestResourceLoader.load(
                "bq_insert_job_with_create_ddl_operation_no_parent.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setTransform(
                            TransformInformation.newBuilder()
                                .setSql(
                                    "CREATE OR REPLACE TABLE `demo.test_partition`\nPARTITION BY DATE(`timestamp`) AS\nSELECT\n'6' as id,\nPARSE_TIMESTAMP('%F','2020-04-12') as `timestamp`\n")
                                .build())
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromLegacyTableName(
                                "myproject:demo.test_partition")
                                .dataEntity())
                        .setOperation("QUERY_JOB")
                        .build())
                .build());
  }

  @Test
  public void extract_ddlCreateStatementOneParent_valid() {

    assertThat(
        new QueryJobExtractor(TestResourceLoader.load("bq_create_ddl_one_parent.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setTransform(
                            TransformInformation.newBuilder()
                                .setSql(
                                    "CREATE OR REPLACE TABLE `myproject.demo.test_partition`\nAS\nSELECT\n timestamp,\n partner_id,\n advertiser_id\nFROM `myproject.demo.transactions`")
                                .build())
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromLegacyTableName(
                                "myproject:demo.test_partition")
                                .dataEntity())
                        .addAllParents(
                            ImmutableSet.of(
                                BigQueryTableCreator.fromBigQueryResource(
                                    "projects/myproject/datasets/demo/tables/transactions")
                                    .dataEntity()))
                        .setOperation("QUERY_JOB")
                        .build())
                .build());
  }

  @Test
  public void extract_noReferencedTables_lineageWithEmptyParents() {
    assertThat(
        new QueryJobExtractor(
            TestResourceLoader.load("bq_last_message_without_reference_tables.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setTransform(
                            TransformInformation.newBuilder()
                                .setSql("SELECT * FROM `myproject.demo.events` LIMIT 10")
                                .build())
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromBigQueryResource(
                                "projects/myproject/datasets/demo/tables/sample_table")
                                .dataEntity())
                        .setOperation("QUERY_JOB")
                        .build())
                .build());
  }

  @Test
  public void extract_validQueryMessage_completedLineage() {
    assertThat(
        new QueryJobExtractor(TestResourceLoader.load("complete_bq_last_message.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setTransform(
                            TransformInformation.newBuilder()
                                .setSql("SELECT keyid FROM `myproject.demo.events` LIMIT 10")
                                .build())
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromBigQueryResource(
                                "projects/myproject/datasets/demo/tables/sample_table")
                                .dataEntity())
                        .addAllParents(
                            ImmutableSet.of(
                                BigQueryTableCreator.fromBigQueryResource(
                                    "projects/myproject/datasets/demo/tables/events")
                                    .dataEntity()))
                        .setOperation("QUERY_JOB")
                        .build())
                .build());
  }

  @Test
  public void extract_validQueryMessageAndCatalog_columnLineage() {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/bigquery_demo_events_schema.json"));
    ZetaSqlSchemaLoaderFactory fakeLoaderFactory =
        (ZetaSqlSchemaLoaderFactory)
            () ->
                new BigQueryZetaSqlSchemaLoader(
                    BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    assertThat(
        new QueryJobExtractor(
            TestResourceLoader.load("complete_bq_last_message.json"), fakeLoaderFactory)
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setTransform(
                            TransformInformation.newBuilder()
                                .setSql("SELECT keyid FROM `myproject.demo.events` LIMIT 10")
                                .build())
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromBigQueryResource(
                                "projects/myproject/datasets/demo/tables/sample_table")
                                .dataEntity())
                        .addAllParents(
                            ImmutableSet.of(
                                BigQueryTableCreator.fromBigQueryResource(
                                    "projects/myproject/datasets/demo/tables/events")
                                    .dataEntity()))
                        .setOperation("QUERY_JOB")
                        .build())
                .addAllColumnsLineage(
                    ImmutableSet.of(
                        ColumnLineage.newBuilder()
                            .setTarget(ColumnEntity.newBuilder().setColumn("keyid").build())
                            .addAllParents(
                                ImmutableSet.of(
                                    ColumnEntity.newBuilder()
                                        .setTable(
                                            BigQueryTableCreator.usingBestEffort(
                                                "myproject.demo.events")
                                                .dataEntity())
                                        .setColumn("keyid")
                                        .build()))
                            .build()))
                .build());
  }
}
