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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.service.BigQueryTableLoadService;
import com.google.cloud.solutions.datalineage.service.BigQueryZetaSqlSchemaLoader;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/*
  //TODO skuehn test plan:
     Update: test nested updates
            update with join: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_statement
     Insert: omitting column names: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#omitting_column_names
             insert with subquery
 */
@RunWith(JUnit4.class)
public final class BigQuerySqlParserTest {

  @Test
  public void queryExtractColumnLineage_concatColumns_correctColumnNames() {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory
            .forTableSchemas(
                TestResourceLoader.load("schemas/tableA_schema.json"),
                TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader.load("sql/kitchen_sink_concat.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(
                    ColumnEntity.newBuilder().setColumn("joined_column").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("project1.datasetA.TableA")
                                .dataEntity())
                            .setColumn("colA").build(),
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("project2.datasetB.TableB")
                                .dataEntity())
                            .setColumn("colB").build()))
                .addAllOperations(ImmutableList.of("CONCAT (ZetaSQL:concat)"))
                .build());
  }

  @Test
  public void queryExtractColumnLineage_multipleOutputColumnsWithAlias_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/tableA_schema.json"),
            TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader.load(
                    "sql/kitchen_sink_multiple_output_columns_with_alias.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(
                    ColumnEntity.newBuilder().setColumn("joined_column").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project1.datasetA.TableA")
                                .dataEntity())
                            .setColumn(
                                /*columnName=*/ "colA").build(),
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project2.datasetB.TableB")
                                .dataEntity())
                            .setColumn("colB").build()))
                .addAllOperations(ImmutableList.of("CONCAT (ZetaSQL:concat)"))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("columnA").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project1.datasetA.TableA")
                                .dataEntity()).setColumn("colA").build()))
                .build());
  }

  @Test
  public void queryExtractColumnLineage_multipleOutputColumnsWithoutAlias_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/tableA_schema.json"),
            TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader
                    .load("sql/kitchen_sink_multiple_output_columns_without_alias.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("joined_column").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project1.datasetA.TableA")
                                .dataEntity())
                            .setColumn("colA").build(),
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project2.datasetB.TableB")
                                .dataEntity())
                            .setColumn("colB").build()))
                .addAllOperations(ImmutableList.of("CONCAT (ZetaSQL:concat)"))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("colB").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort(
                                "project2.datasetB.TableB").dataEntity())
                            .setColumn("colB").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("columnA").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("project1.datasetA.TableA")
                                .dataEntity())
                            .setColumn("colA").build()))
                .build());
  }

  @Test
  public void
  queryExtractColumnLineage_bigQuerySchemaMultipleOutputColumnsWithoutAlias_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/daily_report_table_schema.json"),
            TestResourceLoader.load("schemas/error_stats_table_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader
                    .load(
                        "sql/bigquery_daily_report_error_stats_join_group_by_aggr_functions.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("partner_id").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.reporting.daily_report")
                                .dataEntity())
                            .setColumn("partner_id").build()))
                .addAllOperations(ImmutableList.of("groupBy"))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("total_lines").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.reporting.daily_report")
                                .dataEntity())
                            .setColumn("num_lines").build()))
                .addAllOperations(ImmutableList.of("SUM (ZetaSQL:sum)"))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("total_200").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort("myproject.reporting.error_stats")
                                .dataEntity())
                            .setColumn("status_200").build()))
                .addAllOperations(ImmutableList.of("SUM (ZetaSQL:sum)"))
                .build());
  }

  @Test
  public void extractColumnLineage_publicDatasetQuery_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigQueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/public_dataset_mbb_teams_schema.json"),
            TestResourceLoader.load("schemas/public_dataset_mbb_team_colors_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigQueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader
                    .load(
                        "sql/nbaa_public_dataset_join.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("id").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.mbb_teams")
                                .dataEntity())
                            .setColumn("id").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("alias").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator.usingBestEffort(
                                "bigquery-public-data.ncaa_basketball.mbb_teams").dataEntity())
                            .setColumn("alias").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("code_ncaa").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("code_ncaa").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("hex_color").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("color").build()))
                .build());
  }

  @Test
  public void mergeExtractColumnLineage_publicDatasetQuery_inferColumns_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigQueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/public_dataset_mbb_team_colors_schema_merge.json"),
            TestResourceLoader.load("schemas/public_dataset_mbb_team_colors_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigQueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader
                    .load(
                        "sql/nbaa_public_dataset_merge.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("market").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("market").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("id").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("id").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("code_ncaa").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("code_ncaa").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("color").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("bigquery-public-data.ncaa_basketball.team_colors")
                                .dataEntity())
                            .setColumn("color").build()))
                .build());
  }


  @Test
  public void mergeExtractColumnLineage_publicDatasetQuery_specifyColumns_correctColumnLineage() {
    FakeBigQueryServiceFactory fakeBigQueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/error_stats_agg_table_schema.json"),
            TestResourceLoader.load("schemas/error_stats_table_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigQueryFactory));

    ImmutableSet<ColumnLineage> resolvedStatement =
        new BigQuerySqlParser(fakeSchemaLoader)
            .extractColumnLineage(
                TestResourceLoader
                    .load(
                        "sql/error_stats_merge.sql"));

    assertThat(resolvedStatement)
        .containsExactly(
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("partner_id").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("myproject.reporting.error_stats")
                                .dataEntity())
                            .setColumn("partner_id").build()))
                .build(),
            ColumnLineage.newBuilder()
                .setTarget(ColumnEntity.newBuilder().setColumn("num_hits").build())
                .addAllParents(
                    ImmutableSet.of(
                        ColumnEntity.newBuilder().setTable(
                            BigQueryTableCreator
                                .usingBestEffort("myproject.reporting.error_stats")
                                .dataEntity())
                            .setColumn("num_hits").build()))
                .build());
  }
}