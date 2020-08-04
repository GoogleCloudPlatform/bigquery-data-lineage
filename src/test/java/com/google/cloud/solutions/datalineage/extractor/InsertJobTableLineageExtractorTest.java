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

import com.google.cloud.solutions.datalineage.model.CloudStorageFile;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TransformInformation;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class InsertJobTableLineageExtractorTest {

  private Clock testClock;

  @Before
  public void setUpFixedClock() {
    testClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
  }

  @Test
  public void extract_ddlCreateStatementNoParents_valid() {

    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock,
            TestResourceLoader.load(
                "bq_insert_job_with_create_ddl_operation_no_parent.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setActuator("user@example.com")
                        .setJobTime(Instant.parse("2020-05-07T12:59:30.463Z").toEpochMilli())
                        .setJobId("projects/myproject/jobs/bquxjob_264a99a3_171ef37d030")
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

    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock, TestResourceLoader.load("bq_create_ddl_one_parent.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setActuator("user@example.com")
                        .setJobTime(Instant.parse("2020-05-07T16:23:21.643Z").toEpochMilli())
                        .setJobId("projects/myproject/jobs/bquxjob_671ec7b5_171efefdb82")
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
    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock,
            TestResourceLoader.load("bq_last_message_without_reference_tables.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setJobId("projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4")
                        .setJobTime(Instant.parse("2020-04-13T13:57:08.14Z").toEpochMilli())
                        .setActuator("user@example.com")
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
    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock, TestResourceLoader.load("complete_bq_last_message.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setJobId("projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4")
                        .setJobTime(Instant.parse("2020-04-13T13:57:08.14Z").toEpochMilli())
                        .setActuator("user@example.com")
                        .setTransform(TransformInformation.newBuilder()
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
  public void extract_loadJob_completedLineage() {
    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock, TestResourceLoader.load("bq_file_load_job.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setActuator("user@example.com")
                        .setJobTime(Instant.parse("2020-05-07T18:16:03.906Z").toEpochMilli())
                        .setJobId("projects/bq-lineage-demo/jobs/bquxjob_718728d9_171f059e94d")
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromBigQueryResource(
                                "projects/bq-lineage-demo/datasets/MyDataSet/tables/UserData")
                                .dataEntity())
                        .addAllParents(
                            ImmutableSet.of(
                                CloudStorageFile.create(
                                    "gs://lineage-dataflow-29173/userdata1.parquet")
                                    .dataEntity()))
                        .setOperation("LOAD_JOB")
                        .build())
                .build());
  }

  @Test
  public void extract_copyJobTempTable_empty() {
    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock, TestResourceLoader.load("bq_query_copy_temp_table.json"))
            .extract())
        .isEqualTo(CompositeLineage.getDefaultInstance());
  }

  @Test
  public void extract_copyJob_valid() {
    Truth.assertThat(
        new InsertJobTableLineageExtractor(
            testClock, TestResourceLoader.load("bq_query_copy_table_valid.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setReconcileTime(testClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setActuator("fakeskimo@blueseagulls.com")
                        .setJobTime(Instant.parse("2020-05-08T04:27:54.116Z").toEpochMilli())
                        .setJobId(
                            "projects/helical-client-276602/jobs/bquxjob_49c5a65b_171f28a27d4")
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            BigQueryTableCreator.fromBigQueryResource(
                                "projects/helical-client-276602/datasets/airports/tables/airports_asia")
                                .dataEntity())
                        .addAllParents(
                            ImmutableSet.of(
                                BigQueryTableCreator.fromBigQueryResource(
                                    "projects/helical-client-276602/datasets/my_dataset/tables/MySourceTable")
                                    .dataEntity()))
                        .setOperation("COPY_JOB")
                        .build())
                .build());
  }
}
