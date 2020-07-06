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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.cloud.solutions.datalineage.BigQuerySqlParser;
import com.google.cloud.solutions.datalineage.model.DataEntityConvertible;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.zetasql.SqlException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Lineage Extractor for BigQuery QUERY type jobs. It also uses ZetaSQL based SQL parsing to extract
 * ColumnLineage is available.
 */
public final class QueryJobExtractor extends LineageExtractor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String QUERY_REFERENCED_TABLES = "$.jobChange.job.jobStats.queryStats.referencedTables";
  private static final String QUERY_SQL_PATH = "$.jobChange.job.jobConfig.queryConfig.query";
  private static final String QUERY_DESTINATION_TABLE = "$.jobChange.job.jobConfig.queryConfig.destinationTable";

  private final ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory;

  public QueryJobExtractor(JsonMessageParser messageParser,
      ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory) {
    super(messageParser);
    this.zetaSqlSchemaLoaderFactory = zetaSqlSchemaLoaderFactory;
  }

  public QueryJobExtractor(String messageJson,
      ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory) {
    super(messageJson);
    this.zetaSqlSchemaLoaderFactory = zetaSqlSchemaLoaderFactory;
  }

  public QueryJobExtractor(String messageJson) {
    this(messageJson, ZetaSqlSchemaLoaderFactory.emptyLoaderFactory());
  }

  @Override
  public CompositeLineage extract() {
    return CompositeLineage.newBuilder()
        .setTableLineage(
            TableLineage.newBuilder()
                .setOperation("QUERY_JOB")
                .setTarget(
                    DataEntityConvertible
                        .convert(fromBigQueryResource(metadata().read(QUERY_DESTINATION_TABLE))))
                .addAllParents(extractReferencedTables())
                .build())
        .addAllColumnsLineage(extractColumnLineage())
        .build();
  }

  /**
   * Returns Column level lineage using ZetaSQL based query parser.
   */
  private ImmutableSet<ColumnLineage> extractColumnLineage() {
    try {
      String sql = extractQuerySql();

      if (isBlank(sql)) {
        return ImmutableSet.of();
      }

      return
          new BigQuerySqlParser(zetaSqlSchemaLoaderFactory.newLoader())
              .extractColumnLineage(sql);
    } catch (SqlException sqlException) {
      logger.atWarning()
          .withCause(sqlException)
          .atMostEvery(1, TimeUnit.MINUTES)
          .log();
    }
    return ImmutableSet.of();
  }

  private String extractQuerySql() {
    return metadata().readOrDefault(QUERY_SQL_PATH, "");
  }


  /**
   * Returns a list of BigQuery tables referenced by the query from the Query logs
   */
  private ImmutableSet<DataEntity> extractReferencedTables() {
    if (!metadata().containsKey(QUERY_REFERENCED_TABLES)) {
      return ImmutableSet.of();
    }

    return
        metadata()
            .<List<String>>readOrDefault(QUERY_REFERENCED_TABLES, ImmutableList.of())
            .stream()
            .map(BigQueryTableCreator::fromBigQueryResource)
            .map(DataEntityConvertible::convert)
            .collect(toImmutableSet());
  }
}
