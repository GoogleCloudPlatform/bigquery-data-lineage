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

package com.google.cloud.solutions.datalineage.extractor;

import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import java.time.Clock;

/**
 * Parses BigQuery Job message to identify Job Type to invoke appropriate LineageExtractor.
 */
public final class LineageExtractorIdentifier {

  private static final String LAST_OPERATION_FLAG_PATH = "$.operation.last";
  private static final String JOB_METHOD_TYPE_PATH = "$.protoPayload.methodName";
  private static final String JOB_STATE_PATH = "$.protoPayload.metadata.jobChange.job.jobStatus.jobState";
  private static final String JOB_STATUS_ERRORS_PATH = "$.protoPayload.metadata.jobChange.job.jobStatus.errors";
  private final JsonMessageParser parser;
  private final Clock clock;
  private final ZetaSqlSchemaLoaderFactory schemaLoaderFactory;

  public LineageExtractorIdentifier(String messageJson) {
    this(Clock.systemUTC(), messageJson);
  }

  public LineageExtractorIdentifier(Clock clock, String messageJson) {
    this(clock, messageJson, ZetaSqlSchemaLoaderFactory.emptyLoaderFactory());
  }

  public LineageExtractorIdentifier(
      Clock clock,
      String messageJson,
      ZetaSqlSchemaLoaderFactory schemaLoaderFactory) {
    this.parser = JsonMessageParser.of(messageJson);
    this.clock = clock;
    this.schemaLoaderFactory = schemaLoaderFactory;
  }

  public static LineageExtractorIdentifier of(String messageJson) {
    return new LineageExtractorIdentifier(messageJson);
  }

  /**
   * Returns the JobTypeSpecific extractor or {@link NoOpExtractor} if job type is unregistered.
   */
  public LineageExtractor identify() {

    if (!isJobSuccessful()) {
      return NoOpExtractor.getInstance();
    }

    // This If can be expanded into a switch statement to support more Job types.
    if ("google.cloud.bigquery.v2.JobService.InsertJob".equals(extractJobType())) {
      return new InsertJobTableLineageExtractor(clock, parser, schemaLoaderFactory);
    }
    return NoOpExtractor.getInstance();
  }

  private String extractJobType() {
    return parser.readOrDefault(JOB_METHOD_TYPE_PATH, "");
  }

  private boolean isJobSuccessful() {
    return containsJobStatus() && isDone() && isErrorFree() && isLastOperation();
  }

  private boolean containsJobStatus() {
    return parser.containsKey("$.protoPayload.metadata.jobChange.job.jobStatus");
  }

  private boolean isDone() {
    return parser.readOrDefault(JOB_STATE_PATH, "").equals("DONE");
  }

  private boolean isErrorFree() {
    return !parser.containsKey(JOB_STATUS_ERRORS_PATH);
  }

  private boolean isLastOperation() {
    return parser.readOrDefault(LAST_OPERATION_FLAG_PATH, false);
  }
}
