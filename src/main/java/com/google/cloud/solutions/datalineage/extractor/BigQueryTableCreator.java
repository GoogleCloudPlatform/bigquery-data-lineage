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

import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.FluentLogger;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Factory to create BigQuery table entity by parsing different naming formats.
 */
public abstract class BigQueryTableCreator {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  /**
   * Matches the given String as a Legacy or Standard Table name. If no match found, it returns a
   *
   * @param bigQueryTableName the table name in legacy or standard SQL format.
   * @return Table information with valid values or if no match found then tableName set as the
   * input String or empty table name.
   */
  public static BigQueryTableEntity usingBestEffort(String bigQueryTableName) {
    if (bigQueryTableName != null && bigQueryTableName.startsWith("$")) {
      return BigQueryTableEntity.create(null, null, bigQueryTableName);
    }

    for (String pattern : ImmutableList
        .of(BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT, BQ_RESOURCE_FORMAT, BQ_LINKED_RESOURCE_FORMAT)) {
      try {
        return extractInformation(pattern, bigQueryTableName);
      } catch (IllegalArgumentException aex) {
        logger.atInfo().atMostEvery(1, TimeUnit.MINUTES).withCause(aex)
            .log("error parsing %s", bigQueryTableName);
      }
    }

    throw new IllegalArgumentException(
        "Couldn't convert into any known types: (" + bigQueryTableName + ")");
  }

  /**
   * Returns a parsed TableEntity from the legacy SQL form (<project-id>:<dataset-id>.<table-id>) of
   * a BigQuery table.
   */
  public static BigQueryTableEntity fromLegacyTableName(String legacyName) {
    return extractInformation(LEGACY_TABLE_FORMAT, legacyName);
  }

  public static BigQueryTableEntity fromSqlResource(String sqlResource) {
    return extractInformation(SQL_RESOURCE_FORMAT, sqlResource);
  }

  public static BigQueryTableEntity fromBigQueryResource(String resource) {
    return extractInformation(BQ_RESOURCE_FORMAT, resource);
  }

  public static BigQueryTableEntity fromLinkedResource(String linkedResource) {
    return extractInformation(BQ_LINKED_RESOURCE_FORMAT, linkedResource);
  }

  private static final String PROJECT_ID_TAG = "projectId";
  private static final String DATASET_ID_TAG = "dataset";
  private static final String TABLE_ID_TAG = "table";

  private static final String PROJECT_PATTERN = "[a-zA-Z0-9\\.\\-\\:]+";
  private static final String DATASET_PATTERN = "[a-zA-Z_][a-zA-Z0-9\\_]+";
  private static final String TABLE_PATTERN = "[a-zA-Z][a-zA-Z0-9\\_]+";


  private static final String LEGACY_TABLE_FORMAT =
      String.format(
          "^(?<%s>%s)\\:(?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String SQL_RESOURCE_FORMAT =
      String.format(
          "^bigquery\\.table\\.(?<%s>%s)\\.(?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_RESOURCE_FORMAT =
      String.format(
          "^projects/(?<%s>%s)/datasets/(?<%s>%s)/tables/(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_LINKED_RESOURCE_FORMAT =
      String.format(
          "^//bigquery.googleapis.com/projects/(?<%s>%s)/datasets/(?<%s>%s)/tables/(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT =
      String.format(
          "^(?<%s>%s)[:\\.](?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static BigQueryTableEntity extractInformation(String pattern, String resource) {
    Matcher matcher = Pattern.compile(pattern).matcher(resource);

    if (!matcher.find()) {
      throw new IllegalArgumentException(
          "input (" + resource + ") not in correct format (" + pattern + ")");
    }

    return BigQueryTableEntity.builder()
        .setProjectId(matcher.group(PROJECT_ID_TAG))
        .setDataset(matcher.group(DATASET_ID_TAG))
        .setTable(matcher.group(TABLE_ID_TAG))
        .build();
  }

  private BigQueryTableCreator() {
  }
}
