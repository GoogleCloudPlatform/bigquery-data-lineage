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
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.TagField;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.BigQueryTableLoadService;
import com.google.cloud.solutions.datalineage.service.BigQueryZetaSqlSchemaLoader;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.FakeDataCatalogStub;
import com.google.cloud.solutions.datalineage.testing.JsonTransforms;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.cloud.solutions.datalineage.transform.CompositeLineageToTagTransformation;
import com.google.cloud.solutions.datalineage.transform.LineageExtractionTransform;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jayway.jsonpath.JsonPath;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import net.minidev.json.JSONArray;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PipelineTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  static ImmutableList<String> sampleMessages() {
    return JsonPath
        .parse(
            TestResourceLoader.load("message_sequence_for_bq_query_with_destination_table.json"))
        .<JSONArray>read("$[*]")
        .stream()
        .map(JsonTransforms::stringify)
        .filter(StringUtils::isNotBlank)
        .collect(toImmutableList());
  }

  @Test
  public void compositeLineageSerializable() {
    p.apply(Create.of(CompositeLineage.getDefaultInstance()));
    p.run();
  }

  @Test
  public void tagsForCatalog_empty_Serializable() {
    p.apply(Create.of(TagsForCatalog.empty()));
    p.run();
  }

  @Test
  public void tagsForCatalog_filled_Serializable() {
    TagsForCatalog sample =
        TagsForCatalog
            .forTags(ImmutableSet.of(
                Tag.newBuilder()
                    .setTemplate("templateId")
                    .putFields("test", TagField.newBuilder().setStringValue("string1").build())
                    .build()))
            .setEntry(Entry.newBuilder().setName("asddsdadasda").build())
            .build();

    p.apply(Create.of(sample));
    p.run();
  }

  @Test
  public void auditLogLineageParser_changesToLineageTable_discardChanges() {
    BigQueryTableEntity lineageTable =
        BigQueryTableCreator
            .fromLegacyTableName("myproject:demo.sample_table");

    PCollection<CompositeLineage> output = p
        .apply(Create.of(sampleMessages()))
        .setCoder(StringUtf8Coder.of())
        .apply(LineageExtractionTransform.builder().setOutputLineageTable(lineageTable).build());

    PAssert.that(output).empty();

    p.run();

  }

  @Test
  public void auditLogLineageParser_noSchemaLoader_onlyTableLineage() {
    Clock fixedClock = Clock.fixed(Instant.parse("2020-04-13T21:29:34.923Z"), ZoneId.of("UTC"));
    BigQueryTableEntity lineageTable =
        BigQueryTableCreator
            .fromLegacyTableName("projectId:dataset.table");

    PCollection<CompositeLineage> output = p
        .apply(Create.of(sampleMessages()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            LineageExtractionTransform.builder()
                .setClock(fixedClock)
                .setOutputLineageTable(lineageTable)
                .setZetaSqlSchemaLoaderFactory(ZetaSqlSchemaLoaderFactory.emptyLoaderFactory())
                .build());

    PAssert.that(output)
        .containsInAnyOrder(
            parseJson(TestResourceLoader
                    .load("composite-lineages/only_table_level_lineage.json"),
                CompositeLineage.class));

    p.run();
  }

  @Test
  public void auditLogLineageParser_sqlParserProvided_containsColumnLineages() {
    Clock fixedClock = Clock.fixed(Instant.parse("2020-04-13T21:29:34.923Z"), ZoneId.of("UTC"));
    BigQueryTableEntity lineageTable =
        BigQueryTableCreator
            .fromLegacyTableName("projecId:dataset.table");
    ZetaSqlSchemaLoaderFactory fakeSchemaLoaderFactory =
        (ZetaSqlSchemaLoaderFactory) () ->
            new BigQueryZetaSqlSchemaLoader(
                BigQueryTableLoadService.usingServiceFactory(
                    FakeBigQueryServiceFactory
                        .forTableSchemas(
                            TestResourceLoader.load("schemas/bigquery_demo_events_schema.json"))));

    PCollection<CompositeLineage> output = p
        .apply(Create.of(sampleMessages()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            LineageExtractionTransform.create(lineageTable, fixedClock, fakeSchemaLoaderFactory));

    PAssert.that(output)
        .containsInAnyOrder(
            parseJson(TestResourceLoader
                    .load("composite-lineages/composite_lineage_simple_table_events_join.json"),
                CompositeLineage.class));

    p.run();
  }

  @Test
  public void auditLogLineageParser_validTagsForCatalog() {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        /*entryResourcesNames=*/
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json",
            "datacatalog-objects/OutputTable_entry.json"),
        /*tagsResourceNames=*/
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));

    PCollection<TagsForCatalog> output = p
        .apply(
            Create.of(
                parseJson(
                    TestResourceLoader
                        .load(
                            "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                    CompositeLineage.class)))
        .apply(
            CompositeLineageToTagTransformation.builder()
                .lineageTagTemplateId("tagTemplateId")
                .catalogStub(fakeStub)
                .build());

    PAssert.that(output)
        .containsInAnyOrder(
            TagsForCatalog
                .forTags(parseAsList("[{\n"
                    + "      \"template\": \"tagTemplateId\",\n"
                    + "      \"fields\": {\n"
                    + "        \"reconcileTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"jobTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"actuator\": {\n"
                    + "          \"stringValue\": \"\"\n"
                    + "        },\n"
                    + "        \"parents\": {\n"
                    + "          \"stringValue\": \"[{\\n  \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n  \\\"sqlResource\\\": \\\"bigquery.table.myproject1.mydataset1.TableA\\\",\\n  \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/myproject1/datasets/mydataset1/tables/TableA\\\"\\n},{\\n  \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n  \\\"sqlResource\\\": \\\"bigquery.table.myproject1.reporting.simple_report_view\\\",\\n  \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/myproject1/datasets/reporting/tables/simple_report_view\\\"\\n}]\"\n"
                    + "        }\n"
                    + "      }\n"
                    + "    },{\n"
                    + "      \"template\": \"tagTemplateId\",\n"
                    + "      \"fields\": {\n"
                    + "        \"reconcileTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"jobTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"actuator\": {\n"
                    + "          \"stringValue\": \"\"\n"
                    + "        },\n"
                    + "        \"parents\": {\n"
                    + "          \"stringValue\": \"[{\\n  \\\"table\\\": {\\n    \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n    \\\"sqlResource\\\": \\\"bigquery.table.myproject1.reporting.simple_report_view\\\",\\n    \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/myproject1/datasets/reporting/tables/simple_report_view\\\"\\n  },\\n  \\\"column\\\": \\\"telephone_number\\\"\\n},{\\n  \\\"table\\\": {\\n    \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n    \\\"sqlResource\\\": \\\"bigquery.table.myproject1.mydataset1.TableA\\\",\\n    \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/myproject1/datasets/mydataset1/tables/TableA\\\"\\n  },\\n  \\\"column\\\": \\\"id\\\"\\n}]\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"combined_telephone\"\n"
                    + "    },{\n"
                    + "      \"template\": \"tagTemplateId\",\n"
                    + "      \"fields\": {\n"
                    + "        \"reconcileTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"jobTime\": {\n"
                    + "          \"timestampValue\": \"1970-01-01T00:00:00Z\"\n"
                    + "        },\n"
                    + "        \"actuator\": {\n"
                    + "          \"stringValue\": \"\"\n"
                    + "        },\n"
                    + "        \"parents\": {\n"
                    + "          \"stringValue\": \"[{\\n  \\\"table\\\": {\\n    \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n    \\\"sqlResource\\\": \\\"bigquery.table.myproject1.reporting.simple_report_view\\\",\\n    \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/myproject1/datasets/reporting/tables/simple_report_view\\\"\\n  },\\n  \\\"column\\\": \\\"telephone_number\\\"\\n}]\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"telephone_number\"\n"
                    + "    }]\n", Tag.class))
                .setEntryId(
                    "projects/myproject1/locations/us/entryGroups/@bigquery/entries/OutputTableId")
                .build());

    p.run();
  }
}
