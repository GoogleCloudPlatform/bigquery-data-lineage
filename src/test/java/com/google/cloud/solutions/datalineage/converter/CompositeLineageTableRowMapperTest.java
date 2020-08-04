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

package com.google.cloud.solutions.datalineage.converter;

import static com.google.cloud.solutions.datalineage.testing.GoogleTypesToJsonConverter.convertToJson;
import static com.google.cloud.solutions.datalineage.testing.JsonAssert.assertJsonEquals;
import static com.google.cloud.solutions.datalineage.testing.TestResourceLoader.load;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CompositeLineageTableRowMapperTest {

  @Test
  public void buildCompleteRow_validCompleteRow() {
    CompositeLineage compositeLineage =
        CompositeLineage.newBuilder()
            .setReconcileTime(Instant.parse("2020-04-12T12:00:00Z").toEpochMilli())
            .setJobInformation(JobInformation.newBuilder()
                .setJobTime(Instant.parse("2020-04-12T10:00:00Z").toEpochMilli())
                .setActuator("test-account@gserviceaccount.com")
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
                            .dataEntity(),
                        BigQueryTableCreator
                            .fromSqlResource("bigquery.table.my-project2.dataset2.RefTable2")
                            .dataEntity()))
                    .build())
            .build();

    TableRow completeRow = new CompositeLineageTableRowMapper(compositeLineage).buildCompleteRow();

    assertJsonEquals(convertToJson(completeRow), "{"
        + "  \"reconcileTime\" : \"2020-04-12 12:00:00.000 UTC\","
        + "  \"jobInformation\" : {"
        + "    \"jobId\" : \"myJobId\","
        + "    \"jobTime\" : \"2020-04-12 10:00:00.000 UTC\","
        + "    \"actuator\" : \"test-account@gserviceaccount.com\""
        + "  },"
        + "  \"tableLineage\" : {"
        + "    \"target\" : {\n"
        + "      \"kind\" : \"BIGQUERY_TABLE\",\n"
        + "      \"sqlResource\" : \"bigquery.table.my-project.my_dataset.mytable\",\n"
        + "      \"linkedResource\" : \"//bigquery.googleapis.com/projects/my-project/datasets/my_dataset/tables/mytable\"\n"
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"kind\" : \"BIGQUERY_TABLE\","
        + "      \"sqlResource\" : \"bigquery.table.my-project2.dataset2.RefTable\","
        + "      \"linkedResource\" : \"//bigquery.googleapis.com/projects/my-project2/datasets/dataset2/tables/RefTable\""
        + "    }, {"
        + "      \"kind\" : \"BIGQUERY_TABLE\","
        + "      \"sqlResource\" : \"bigquery.table.my-project2.dataset2.RefTable2\","
        + "      \"linkedResource\" : \"//bigquery.googleapis.com/projects/my-project2/datasets/dataset2/tables/RefTable2\""
        + "    } ]"
        + "  }"
        + "}");


  }

  @Test
  public void convert_containsColumnLineage_valid() {
    CompositeLineage allLineageComponents = CompositeLineage.newBuilder()
        .setReconcileTime(Instant.parse("2020-04-14T13:57:08.14Z").toEpochMilli())
        .setJobInformation(
            JobInformation.newBuilder()
                .setJobId(
                    "projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4")
                .setJobTime(Instant.parse("2020-04-13T13:57:08.14Z").toEpochMilli())
                .setActuator("user@example.com")
                .build())
        .setTableLineage(
            TableLineage.newBuilder()
                .setTarget(
                    BigQueryTableCreator
                        .fromBigQueryResource(
                            "projects/myproject/datasets/demo/tables/sample_table")
                        .dataEntity())
                .addAllParents(ImmutableSet.of(
                    BigQueryTableCreator
                        .fromBigQueryResource(
                            "projects/myproject/datasets/demo/tables/events")
                        .dataEntity()))
                .setOperation("QUERY_JOB")
                .build())
        .addAllColumnsLineage(
            ImmutableSet.of(
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("transaction_time").build())
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder()
                            .setTable(
                                BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                    .dataEntity())
                            .setColumn("transaction_time")
                            .build()))
                    .build(),
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("keyid").build())
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder()
                            .setTable(BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                .dataEntity())
                            .setColumn("keyid").build()))
                    .build(),
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("lat").build())
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                .dataEntity())
                            .setColumn("lat").build()))
                    .build(),
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("lon").build())
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                .dataEntity())
                            .setColumn("lon").build()))
                    .build(),
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("id"))
                    .addAllOperations(ImmutableList.of("CONCAT", "+"))
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                .dataEntity())
                            .setColumn("id").build()))
                    .build(),
                ColumnLineage.newBuilder()
                    .setTarget(ColumnEntity.newBuilder().setColumn("bucket"))
                    .addAllParents(ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.demo.events")
                                .dataEntity())
                            .setColumn("bucket").build()))
                    .build()))
        .build();

    TableRow completeRow =
        new CompositeLineageTableRowMapper(allLineageComponents)
            .buildCompleteRow();

    assertJsonEquals(convertToJson(completeRow), "{"
        + "  \"reconcileTime\" : \"2020-04-14 13:57:08.140 UTC\","
        + "  \"jobInformation\" : {"
        + "    \"jobTime\" : \"2020-04-13 13:57:08.140 UTC\","
        + "    \"actuator\" : \"user@example.com\","
        + "    \"jobId\" : \"projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4\""
        + "  },"
        + "  \"tableLineage\" : {"
        + "    \"target\" : {\n"
        + "      \"kind\" : \"BIGQUERY_TABLE\",\n"
        + "      \"sqlResource\" : \"bigquery.table.myproject.demo.sample_table\",\n"
        + "      \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/sample_table\"\n"
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"kind\" : \"BIGQUERY_TABLE\","
        + "      \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "      \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "    } ],"
        + "    \"operation\" : \"QUERY_JOB\""
        + "  },"
        + "  \"columnLineage\" : [ {"
        + "    \"target\" : {"
        + "      \"column\" : \"transaction_time\""
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"transaction_time\""
        + "    } ]"
        + "  }, {"
        + "    \"target\" : {"
        + "      \"column\" : \"keyid\""
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"keyid\""
        + "    } ]"
        + "  }, {"
        + "    \"target\" : {"
        + "      \"column\" : \"lat\""
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"lat\""
        + "    } ]"
        + "  }, {"
        + "    \"target\" : {"
        + "      \"column\" : \"lon\""
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"lon\""
        + "    } ]"
        + "  }, {"
        + "    \"target\" : {"
        + "      \"column\" : \"id\""
        + "    },"
        + "    \"operations\" : [ \"CONCAT\", \"+\" ],"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"id\""
        + "    } ]"
        + "  }, {"
        + "    \"target\" : {"
        + "      \"column\" : \"bucket\""
        + "    },"
        + "    \"parents\" : [ {"
        + "      \"table\" : {"
        + "        \"kind\" : \"BIGQUERY_TABLE\","
        + "        \"sqlResource\" : \"bigquery.table.myproject.demo.events\","
        + "        \"linkedResource\" : \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\""
        + "      },"
        + "      \"column\" : \"bucket\""
        + "    } ]"
        + "  } ]"
        + "}");
  }

  @Test
  public void convert_containsJobInformationWithTransform_valid() {
    CompositeLineage jobInfoWithTransform =
        ProtoJsonConverter.parseJson(
            load("composite-lineages/composite_lineage_simple_table_events_join.json"),
            CompositeLineage.class);

    TableRow convertedRow = CompositeLineageTableRowMapper.newMapper().apply(jobInfoWithTransform);

    assertJsonEquals(
        convertToJson(convertedRow),
        "{\n"
            + "  \"reconcileTime\": \"2020-04-13 21:29:34.923 UTC\",\n"
            + "  \"jobInformation\": {\n"
            + "    \"jobId\": \"projects/myproject/jobs/bquxjob_4103dfda_17173ccc9a4\",\n"
            + "    \"jobTime\": \"2020-04-13 13:57:08.140 UTC\",\n"
            + "    \"actuator\": \"user@example.com\",\n"
            + "    \"transform\" : {\n"
            + "      \"sql\" : \"SELECT * FROM `myproject.demo.events` LIMIT 10\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"tableLineage\": {\n"
            + "    \"target\": {\n"
            + "      \"kind\": \"BIGQUERY_TABLE\",\n"
            + "      \"sqlResource\": \"bigquery.table.myproject.demo.sample_table\",\n"
            + "      \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/sample_table\"\n"
            + "    },\n"
            + "    \"operation\": \"QUERY_JOB\",\n"
            + "    \"parents\": [\n"
            + "      {\n"
            + "        \"kind\": \"BIGQUERY_TABLE\",\n"
            + "        \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "        \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"columnLineage\": [\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"transaction_time\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"transaction_time\"\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"keyid\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"keyid\"\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"lat\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"lat\"\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"lon\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"lon\"\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"id\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"id\"\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"target\": {\n"
            + "        \"column\": \"bucket\"\n"
            + "      },\n"
            + "      \"parents\": [\n"
            + "        {\n"
            + "          \"table\": {\n"
            + "            \"kind\": \"BIGQUERY_TABLE\",\n"
            + "            \"sqlResource\": \"bigquery.table.myproject.demo.events\",\n"
            + "            \"linkedResource\": \"//bigquery.googleapis.com/projects/myproject/datasets/demo/tables/events\"\n"
            + "          },\n"
            + "          \"column\": \"bucket\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}\n");
  }
}
