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

package com.google.cloud.solutions.datalineage.transform;

import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.service.BigQueryZetaSqlSchemaLoaderFactory;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class LineageExtractionTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void expand_singleTableQuery_validLineage() {
    Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");
    ZetaSqlSchemaLoaderFactory schemaLoaderFactory =
        BigQueryZetaSqlSchemaLoaderFactory.usingServiceFactory(
            FakeBigQueryServiceFactory.forTableSchemas(
                TestResourceLoader.load("schemas/MyDataSet_PartnerInformation_schema.json"),
                TestResourceLoader
                    .load("schemas/MyDataSet_ExtractionPartnerInformation_schema.json")));

    PCollection<CompositeLineage> lineage =
        p.apply(Create.of(TestResourceLoader.load("bq_query_with_pii_and_other_tags.json")))
            .apply(
                LineageExtractionTransform.create(lineageTable, fixedClock, schemaLoaderFactory));

    PAssert.that(lineage)
        .containsInAnyOrder(
            CompositeLineage.newBuilder()
                .setReconcileTime(fixedClock.instant().toEpochMilli())
                .setJobInformation(
                    JobInformation.newBuilder()
                        .setJobId("projects/bq-lineage-demo/jobs/bquxjob_4288f388_1726f873d79")
                        .setActuator("user@example.com")
                        .setJobTime(1591010148007L)
                        .build())
                .setTableLineage(
                    TableLineage.newBuilder()
                        .setTarget(
                            DataEntity.newBuilder()
                                .setSqlResource(
                                    "bigquery.table.bq-lineage-demo.MyDataSet.ExtractedPartnerInformation")
                                .setLinkedResource(
                                    "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/MyDataSet/tables/ExtractedPartnerInformation")
                                .setKind(DataEntityTypes.BIGQUERY_TABLE)
                                .build())
                        .addParents(
                            DataEntity.newBuilder()
                                .setSqlResource(
                                    "bigquery.table.bq-lineage-demo.MyDataSet.PartnerInformation")
                                .setLinkedResource(
                                    "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/MyDataSet/tables/PartnerInformation")
                                .setKind(DataEntityTypes.BIGQUERY_TABLE)
                                .build())
                        .setOperation("QUERY_JOB")
                        .build())
                .addColumnsLineage(
                    ColumnLineage.newBuilder()
                        .setTarget(
                            ColumnEntity.newBuilder()
                                .setColumn("partner_id")
                                .build())
                        .addParents(
                            ColumnEntity.newBuilder()
                                .setTable(
                                    DataEntity.newBuilder()
                                        .setSqlResource(
                                            "bigquery.table.bq-lineage-demo.MyDataSet.PartnerInformation")
                                        .setLinkedResource(
                                            "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/MyDataSet/tables/PartnerInformation")
                                        .setKind(DataEntityTypes.BIGQUERY_TABLE)
                                        .build())
                                .setColumn("partner_id")
                                .build())
                        .build())
                .addColumnsLineage(
                    ColumnLineage.newBuilder()
                        .setTarget(
                            ColumnEntity.newBuilder()
                                .setColumn("partner_name")
                                .build())
                        .addParents(
                            ColumnEntity.newBuilder()
                                .setTable(
                                    DataEntity.newBuilder()
                                        .setSqlResource(
                                            "bigquery.table.bq-lineage-demo.MyDataSet.PartnerInformation")
                                        .setLinkedResource(
                                            "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/MyDataSet/tables/PartnerInformation")
                                        .setKind(DataEntityTypes.BIGQUERY_TABLE)
                                        .build())
                                .setColumn("partner_name")
                                .build())
                        .build())
                .addColumnsLineage(
                    ColumnLineage.newBuilder()
                        .setTarget(
                            ColumnEntity.newBuilder()
                                .setColumn("partner_phone_number")
                                .build())
                        .addParents(
                            ColumnEntity.newBuilder()
                                .setTable(
                                    DataEntity.newBuilder()
                                        .setSqlResource(
                                            "bigquery.table.bq-lineage-demo.MyDataSet.PartnerInformation")
                                        .setLinkedResource(
                                            "//bigquery.googleapis.com/projects/bq-lineage-demo/datasets/MyDataSet/tables/PartnerInformation")
                                        .setKind(DataEntityTypes.BIGQUERY_TABLE)
                                        .build())
                                .setColumn("partner_phone_number")
                                .build())
                        .build())
                .build());

    p.run();
  }

  @Test
  public void identify_notInsertJob_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(TestResourceLoader.load("get_query_results_message.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }

  @Test
  public void identify_insertJob_insertJobExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");
    Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(TestResourceLoader.load("complete_bq_last_message.json")))
            .apply(
                LineageExtractionTransform.builder().setClock(fixedClock)
                    .setOutputLineageTable(lineageTable).build());

    PAssert.thatSingleton(compositeLineage)
        .isEqualTo(CompositeLineage.newBuilder()
            .setReconcileTime(fixedClock.instant().toEpochMilli())
            .setJobInformation(JobInformation.newBuilder()
                .setJobId("projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4")
                .setJobTime(1586786228140L)
                .setActuator("user@example.com")
                .build())
            .setTableLineage(
                TableLineage.newBuilder()
                    .setOperation("QUERY_JOB")
                    .setTarget(
                        BigQueryTableEntity
                            .create("myproject", "demo", "sample_table").dataEntity())
                    .addParents(
                        BigQueryTableEntity
                            .create("myproject", "demo", "events").dataEntity())
                    .build()
            )
            .build());

    p.run();
  }

  @Test
  public void identify_jobError_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(TestResourceLoader.load("bq_ddl_statement_job_error.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }

  @Test
  public void identify_readTableOperation_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(TestResourceLoader.load("bq_insert_job_readTable_operation.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }

  @Test
  public void identify_nonLastMessage_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(
            TestResourceLoader.load("complete_bq_first_message.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }

  @Test
  public void identify_noMetadata_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(
            TestResourceLoader.load("bq_message_without_metadata.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }

  @Test
  public void identify_tableDeleteJob_noOpExtractor() {
    BigQueryTableEntity lineageTable = BigQueryTableEntity
        .create("myproject", "audit_dataset", "Lineage");

    PCollection<CompositeLineage> compositeLineage =
        p.apply(Create.of(
            TestResourceLoader.load("bq_delete_table_message.json")))
            .apply(
                LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(compositeLineage).empty();

    p.run();
  }
}
