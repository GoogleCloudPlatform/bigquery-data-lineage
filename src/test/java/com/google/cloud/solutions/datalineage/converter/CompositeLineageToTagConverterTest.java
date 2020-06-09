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

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.asJsonString;
import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseJson;
import static com.google.cloud.solutions.datalineage.testing.JsonAssert.assertJsonEquals;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.TagField;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CompositeLineageToTagConverterTest {

  @Test
  public void withLineage_emptyTemplateId_throwsIllegalArgumentException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class,
            () -> LineageToTagConverterFactory.forTemplateId(""));

    assertThat(illegalArgumentException).hasMessageThat()
        .contains("Provide non-blank tagTemplateName");
  }

  @Test
  public void withLineage_nullTemplateId_throwsIllegalArgumentException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class,
            () -> LineageToTagConverterFactory.forTemplateId(null));

    assertThat(illegalArgumentException).hasMessageThat()
        .contains("Provide non-blank tagTemplateName");
  }

  @Test
  public void withLineage_blankTemplateId_throwsIllegalArgumentException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class,
            () -> LineageToTagConverterFactory.forTemplateId("  "));

    assertThat(illegalArgumentException).hasMessageThat()
        .contains("Provide non-blank tagTemplateName");
  }

  @Test
  public void buildTags_onlyTableLineage_containsOneTag() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .build())
        .setTableLineage(TableLineage.newBuilder()
            .setTarget(
                BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                    .dataEntity())
            .build())
        .build();

    CompositeLineageToTagConverter converter = new CompositeLineageToTagConverter("templateId",
        sampleLineage);

    assertThat(converter.buildTags()).hasSize(1);
  }

  @Test
  public void buildTags_onlyTableLineage_containsValidTimestamp() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .build())
        .setTableLineage(TableLineage.newBuilder()
            .setTarget(
                BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                    .dataEntity())
            .build())
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(1);
    assertThat(tags)
        .containsExactly(
            parseJson("{\n"
                + "      \"template\": \"templateId\",\n"
                + "      \"fields\": {\n"
                + "        \"jobTime\": {\n"
                + "          \"timestampValue\": \"2020-04-12T10:00:00Z\"\n"
                + "        },\n"
                + "        \"reconcileTime\": {\n"
                + "          \"timestampValue\": \"2020-04-12T12:00:00Z\"\n"
                + "        },\n"
                + "        \"actuator\": {\n"
                + "          \"stringValue\": \"test-account@gserviceaccount.com\"\n"
                + "        }\n"
                + "      }\n"
                + "    }", Tag.class));

  }

  @Test
  public void buildTags_tableLineagetwoParents_parentsSeparatedByComma() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .setActuator("test-account@gserviceaccount.com")
            .build())
        .setTableLineage(TableLineage.newBuilder()
            .setTarget(
                BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                    .dataEntity())
            .addAllParents(ImmutableSet.of(
                BigQueryTableCreator.fromSqlResource("bigquery.table.my-project2.dataset2.RefTable")
                    .dataEntity(),
                BigQueryTableCreator
                    .fromSqlResource("bigquery.table.my-project2.dataset2.RefTable2")
                    .dataEntity())).build())
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(1);
    assertThat(tags.asList().get(0).getFieldsMap().get("parents").getStringValue())
        .isEqualTo("[{\n"
            + "  \"kind\": \"BIGQUERY_TABLE\",\n"
            + "  \"sqlResource\": \"bigquery.table.my-project2.dataset2.RefTable\",\n"
            + "  \"linkedResource\": \"//bigquery.googleapis.com/projects/my-project2/datasets/dataset2/tables/RefTable\"\n"
            + "},{\n"
            + "  \"kind\": \"BIGQUERY_TABLE\",\n"
            + "  \"sqlResource\": \"bigquery.table.my-project2.dataset2.RefTable2\",\n"
            + "  \"linkedResource\": \"//bigquery.googleapis.com/projects/my-project2/datasets/dataset2/tables/RefTable2\"\n"
            + "}]");
  }

  @Test
  public void buildTags_jobId() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .setJobId("projects/projectId/bigquery/jobId")
            .build())
        .setTableLineage(TableLineage.newBuilder()
            .setTarget(
                BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                    .dataEntity())
            .build())
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(1);

    Map<String, TagField> tagFieldMap = tags.asList().get(0).getFieldsMap();

    assertThat(tagFieldMap).containsKey("jobId");
    assertThat(tagFieldMap.get("jobId").getStringValue())
        .isEqualTo("projects/projectId/bigquery/jobId");
  }

  @Test
  public void buildTags_containsTagTemplateId() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobId("projects/projectId/bigquery/jobId")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .build())
        .setTableLineage(TableLineage.newBuilder().setTarget(
            BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                .dataEntity()).build())
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(1);
    assertThat(tags.asList().get(0).getTemplate()).isEqualTo("templateId");
  }

  @Test
  public void buildTags_noParent_tagDoesNotContainParent() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .build())
        .setTableLineage(TableLineage.newBuilder().setTarget(
            BigQueryTableCreator.fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                .dataEntity()).build())
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(1);
    assertJsonEquals(asJsonString(tags),
        "[{\n"
            + "  \"template\": \"templateId\",\n"
            + "  \"fields\": {\n"
            + "    \"jobTime\": {\n"
            + "      \"timestampValue\": \"2020-04-12T10:00:00Z\"\n"
            + "    },\n"
            + "    \"reconcileTime\": {\n"
            + "      \"timestampValue\": \"2020-04-12T12:00:00Z\"\n"
            + "    },\n"
            + "    \"actuator\": {\n"
            + "      \"stringValue\": \"test-account@gserviceaccount.com\"\n"
            + "    }\n"
            + "  }\n"
            + "}]");

  }

  @Test
  public void buildTags_columnLineage_valid() {
    CompositeLineage sampleLineage = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
        .setJobInformation(JobInformation.newBuilder()
            .setActuator("test-account@gserviceaccount.com")
            .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
            .setJobId("myJobId")
            .build())
        .setTableLineage(
            TableLineage.newBuilder()
                .setTarget(
                    BigQueryTableCreator
                        .fromSqlResource("bigquery.table.my-project.my_dataset.mytable")
                        .dataEntity())
                .addAllParents(ImmutableSet.of(
                    BigQueryTableCreator
                        .fromSqlResource("bigquery.table.my-project2.dataset2.RefTable")
                        .dataEntity()))
                .build())
        .addAllColumnsLineage(ImmutableList.of(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("outCol").build())
                .addAllParents(ImmutableSet.of(
                    ColumnEntity.newBuilder().setTable(
                        BigQueryTableCreator
                            .usingBestEffort("bigquery.table.my-project2.dataset2.RefTable")
                            .dataEntity())
                        .setColumn("inCol1").build()))
                .build()))
        .build();

    ImmutableSet<Tag> tags =
        new CompositeLineageToTagConverter("templateId", sampleLineage)
            .buildTags();

    assertThat(tags).hasSize(2);
    assertJsonEquals(asJsonString(tags),
        "[{\n"
            + "   \"template\": \"templateId\",\n"
            + "   \"fields\": {\n"
            + "    \"jobTime\": {\n"
            + "     \"timestampValue\": \"2020-04-12T10:00:00Z\"\n"
            + "    },\n"
            + "    \"reconcileTime\": {\n"
            + "     \"timestampValue\": \"2020-04-12T12:00:00Z\"\n"
            + "    },\n"
            + "    \"jobId\": {\n"
            + "      \"stringValue\": \"myJobId\"\n"
            + "    },"
            + "    \"actuator\": {\n"
            + "     \"stringValue\": \"test-account@gserviceaccount.com\"\n"
            + "    },\n"
            + "    \"parents\": {\n"
            + "     \"stringValue\": \"[{\\n  \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n  \\\"sqlResource\\\": \\\"bigquery.table.my-project2.dataset2.RefTable\\\",\\n  \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/my-project2/datasets/dataset2/tables/RefTable\\\"\\n}]\"\n"
            + "    }\n"
            + "   }\n"
            + "  },\n"
            + "  {\n"
            + "   \"template\": \"templateId\",\n"
            + "   \"fields\": {\n"
            + "    \"jobTime\": {\n"
            + "     \"timestampValue\": \"2020-04-12T10:00:00Z\"\n"
            + "    },\n"
            + "    \"reconcileTime\": {\n"
            + "     \"timestampValue\": \"2020-04-12T12:00:00Z\"\n"
            + "    },\n"
            + "    \"actuator\": {\n"
            + "     \"stringValue\": \"test-account@gserviceaccount.com\"\n"
            + "    },\n"
            + "    \"jobId\": {\n"
            + "      \"stringValue\": \"myJobId\"\n"
            + "    },"
            + "    \"parents\": {\n"
            + "     \"stringValue\": \"[{\\n  \\\"table\\\": {\\n    \\\"kind\\\": \\\"BIGQUERY_TABLE\\\",\\n    \\\"sqlResource\\\": \\\"bigquery.table.bigquery.table.my-project2.dataset2.RefTable\\\",\\n    \\\"linkedResource\\\": \\\"//bigquery.googleapis.com/projects/bigquery.table.my-project2/datasets/dataset2/tables/RefTable\\\"\\n  },\\n  \\\"column\\\": \\\"inCol1\\\"\\n}]\"\n"
            + "    }\n"
            + "   },\n"
            + "   \"column\": \"outCol\"\n"
            + "  }]");
  }


  @Test
  public void buildTags_simpleViewPiiTags_valid() {
    CompositeLineage sampleLineage = parseJson(
        TestResourceLoader
            .load(
                "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
        CompositeLineage.class);

    assert sampleLineage != null;
    ImmutableSet<Tag> tags = new CompositeLineageToTagConverter("tagTemplateId", sampleLineage)
        .buildTags();
    assertJsonEquals(asJsonString(tags),
        "[{\n"
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
            + "    }]\n");
  }

}
