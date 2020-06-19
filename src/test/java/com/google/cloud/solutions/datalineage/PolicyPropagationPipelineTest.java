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

package com.google.cloud.solutions.datalineage;

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseAsList;
import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseJson;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnPolicyTags;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TargetPolicyTags;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.FakeDataCatalogStub;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.cloud.solutions.datalineage.transform.CatalogTagsPropagationTransform;
import com.google.cloud.solutions.datalineage.transform.PolicyTagsPropagationTransform;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PolicyPropagationPipelineTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void tagPropagation_noMonitoredTags_targetEntryIdWithEmptyTags() {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json",
            "datacatalog-objects/OutputTable_entry.json"),
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));

    PCollection<TagsForCatalog> tags =
        testPipeline
            .apply(Create.of(
                parseJson(TestResourceLoader.load(
                    "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                    CompositeLineage.class)))
            .apply(
                CatalogTagsPropagationTransform
                    .builder()
                    .catalogStub(fakeStub)
                    .build());

    PAssert.that(tags).empty();

    testPipeline.run();
  }

  @Test
  public void tagPropagation_compositeLineagePiiTags_valid() {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json",
            "datacatalog-objects/OutputTable_entry.json"),
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));
    ImmutableList<String> monitoredTags = ImmutableList.of(
        "projects/myproject1/locations/us-central1/tagTemplates/pii_tag",
        "projects/myproject1/locations/us-central1/tagTemplates/pii_tag2");

    PCollection<TagsForCatalog> tags =
        testPipeline
            .apply(Create.of(
                parseJson(TestResourceLoader.load(
                    "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                    CompositeLineage.class)))
            .apply(
                CatalogTagsPropagationTransform
                    .forMonitoredTags(monitoredTags)
                    .catalogStub(fakeStub)
                    .build());

    PAssert.that(tags)
        .containsInAnyOrder(
            TagsForCatalog
                .forTags(parseAsList(
                    TestResourceLoader.load("tags_for_catalog_samples/pii_template_tags.json"),
                    Tag.class))
                .setEntryId(
                    "projects/myproject1/locations/us/entryGroups/@bigquery/entries/OutputTableId")
                .build());

    testPipeline.run();
  }

  @Test
  public void tagPropagation_nullMonitoredTags_empty() {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json",
            "datacatalog-objects/OutputTable_entry.json"),
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));

    PCollection<TagsForCatalog> tags =
        testPipeline
            .apply(Create.of(
                parseJson(TestResourceLoader.load(
                    "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                    CompositeLineage.class)))
            .apply(
                CatalogTagsPropagationTransform
                    .forMonitoredTags(null)
                    .catalogStub(fakeStub)
                    .build());

    PAssert.that(tags).empty();

    testPipeline.run();
  }

  @Test
  public void policyTagsPropagationTransform_emptyMonitoredTags_empty() {
    String[] tableSchemas = new String[]{
        TestResourceLoader.load("schemas/CorePii_schema.json"),
        TestResourceLoader.load("schemas/SensitiveData_schema.json"),
        TestResourceLoader.load("schemas/OutputTable_schema.json")};
    CompositeLineage testLineage =
        parseJson(
            TestResourceLoader
                .load(
                    "composite-lineages/CorePii_SensitiveData_OutputTable_policyTags.json"),
            CompositeLineage.class);

    PCollection<TargetPolicyTags> targetPolicyTags =
        testPipeline
            .apply(Create.of(testLineage))
            .apply(PolicyTagsPropagationTransform.builder()
                .bigQueryServiceFactory(FakeBigQueryServiceFactory.forTableSchemas(tableSchemas))
                .build());

    PAssert.that(targetPolicyTags).empty();

    testPipeline.run();
  }

  @Test
  public void policyTagsPropagationTransform_monitoredTags_valid() {
    String[] tableSchemas = new String[]{
        TestResourceLoader.load("schemas/CorePii_schema.json"),
        TestResourceLoader.load("schemas/SensitiveData_schema.json"),
        TestResourceLoader.load("schemas/OutputTable_schema.json")};
    CompositeLineage testLineage =
        parseJson(
            TestResourceLoader
                .load(
                    "composite-lineages/CorePii_SensitiveData_OutputTable_policyTags.json"),
            CompositeLineage.class);

    PCollection<TargetPolicyTags> targetPolicyTags =
        testPipeline
            .apply(Create.of(testLineage))
            .apply(PolicyTagsPropagationTransform.builder()
                .monitoredPolicyTags(ImmutableList.of(
                    "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/1234",
                    "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/7890"))
                .bigQueryServiceFactory(FakeBigQueryServiceFactory.forTableSchemas(tableSchemas))
                .build());

    PAssert.that(targetPolicyTags)
        .containsInAnyOrder(
            TargetPolicyTags.newBuilder()
                .setTable(
                    BigQueryTableEntity
                        .create("demoProject", "demo", "OutputTable")
                        .dataEntity())
                .addPolicyTags(
                    ColumnPolicyTags.newBuilder()
                        .setColumn("combined_telephone")
                        .addPolicyTagIds(
                            "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/7890")
                        .addPolicyTagIds(
                            "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/1234")
                        .build())
                .addPolicyTags(
                    ColumnPolicyTags.newBuilder().setColumn("telephone_number")
                        .addPolicyTagIds(
                            "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/1234")
                        .build())
                .build());

    testPipeline.run();
  }

  @Test
  public void policyTagsPropagationTransform_monitoredTagsSingleParent_valid() {
    String[] tableSchemas = new String[]{
        TestResourceLoader.load("schemas/MyDataSet_PartnerInformation_schema.json"),
        TestResourceLoader
            .load("schemas/MyDataSet_ExtractionPartnerInformation_schema.json")};

    CompositeLineage testLineage =
        CompositeLineage.newBuilder()
            .setReconcileTime(1591058253563L)
            .setJobInformation(
                JobInformation.newBuilder()
                    .setJobId("projects/bq-lineage-demo/jobs/bquxjob_4288f388_1726f873d79")
                    .setActuator("anantd@google.com")
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
            .build();

    PCollection<TargetPolicyTags> targetPolicyTags =
        testPipeline
            .apply(Create.of(testLineage))
            .apply(PolicyTagsPropagationTransform.builder()
                .monitoredPolicyTags(ImmutableList.of(
                    "projects/bq-lineage-demo/locations/us/taxonomies/544279842572406327/policyTags/2123206183673327057"))
                .bigQueryServiceFactory(FakeBigQueryServiceFactory.forTableSchemas(tableSchemas))
                .build());

    PAssert.that(targetPolicyTags)
        .containsInAnyOrder(
            TargetPolicyTags.newBuilder()
                .setTable(
                    BigQueryTableEntity
                        .create("bq-lineage-demo", "MyDataSet", "ExtractedPartnerInformation")
                        .dataEntity())
                .addPolicyTags(
                    ColumnPolicyTags.newBuilder()
                        .setColumn("partner_id")
                        .addPolicyTagIds(
                            "projects/bq-lineage-demo/locations/us/taxonomies/544279842572406327/policyTags/2123206183673327057")
                        .build())
                .build());

    testPipeline.run();
  }
}