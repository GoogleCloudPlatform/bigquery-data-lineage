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

package com.google.cloud.solutions.datalineage.service;

import com.google.common.flogger.FluentLogger;
import java.util.concurrent.TimeUnit;

/**
 * Factory to build BigQuerySchemaLoaders by instantiating BigQuery service using the provided
 * Credentials.
 */
public final class BigQueryZetaSqlSchemaLoaderFactory implements ZetaSqlSchemaLoaderFactory {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final BigQueryServiceFactory bigQueryServiceFactory;

  public BigQueryZetaSqlSchemaLoaderFactory(BigQueryServiceFactory bigQueryServiceFactory) {
    this.bigQueryServiceFactory = bigQueryServiceFactory;
  }

  public static BigQueryZetaSqlSchemaLoaderFactory usingServiceFactory(
      BigQueryServiceFactory bigQueryServiceFactory) {
    return new BigQueryZetaSqlSchemaLoaderFactory(bigQueryServiceFactory);
  }

  @Override
  public ZetaSqlSchemaLoader newLoader() {
    try {
      return new BigQueryZetaSqlSchemaLoader(
          BigQueryTableLoadService
              .usingServiceFactory(bigQueryServiceFactory));
    } catch (RuntimeException exception) {
      logger.atWarning()
          .withCause(exception)
          .atMostEvery(10, TimeUnit.MINUTES)
          .log("unable to create Bigquery service.");

      return null;
    }
  }
}
