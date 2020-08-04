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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import java.time.Clock;
import java.time.Instant;

/**
 * Identifies Type of Job and invokes appropriate Extractor for capturing lineage information.
 */
public final class InsertJobTableLineageExtractor extends LineageExtractor {

  private static final String JOB_ID = "$.protoPayload.resourceName";
  private static final String JOB_TYPE = "$.protoPayload.metadata.jobChange.job.jobConfig.type";
  private static final String TIMESTAMP = "$.timestamp";
  private static final String PRINCIPAL_EMAIL_PATH =
      "$.protoPayload.authenticationInfo.principalEmail";
  private final Clock clock;
  private final ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory;

  public InsertJobTableLineageExtractor(Clock clock, JsonMessageParser messageParser) {
    this(clock, messageParser, ZetaSqlSchemaLoaderFactory.emptyLoaderFactory());
  }

  public InsertJobTableLineageExtractor(Clock clock, String messageJson) {
    this(clock, JsonMessageParser.of(messageJson));
  }

  public InsertJobTableLineageExtractor(
      Clock clock,
      JsonMessageParser messageParser,
      ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory) {
    super(messageParser);
    this.clock = clock;
    this.zetaSqlSchemaLoaderFactory = zetaSqlSchemaLoaderFactory;
  }

  @Override
  public CompositeLineage extract() {
    CompositeLineage lineage = identifyExtractor().extract();

    return (CompositeLineage.getDefaultInstance().equals(lineage))
        ? lineage
        : addJobInformation(lineage);
  }

  private CompositeLineage addJobInformation(CompositeLineage lineage) {
    return lineage.toBuilder()
        .setJobInformation(buildJobInformation(lineage.getJobInformation()))
        .setReconcileTime(Instant.now(clock).toEpochMilli())
        .build();
  }

  /**
   * Use {@code JOB_TYPE} attribute to identify BigQuery Job type.
   *
   * @return Configured Extractor for the job type of a NoOp extractor.
   */
  private LineageExtractor identifyExtractor() {

    switch (extractJobType()) {
      case "COPY":
        return new CopyJobExtractor(messageParser);

      case "IMPORT":
        return new LoadJobExtractor(messageParser);

      case "QUERY":
        return new QueryJobExtractor(messageParser, zetaSqlSchemaLoaderFactory);
      default:
        return NoOpExtractor.getInstance();
    }
  }

  /**
   * Return a filled JobInformation by parsing the principal, job end time and present time as
   * reconcile time.
   */
  private JobInformation buildJobInformation(JobInformation partialJobInformation) {
    JobInformation basicJobInformation =
        JobInformation.newBuilder()
            .setActuator(extractActuator())
            .setJobId(extractJobId())
            .setJobTime(extractTimestamp().toEpochMilli())
            .build();

    if (partialJobInformation != null) {
      return partialJobInformation.toBuilder().mergeFrom(basicJobInformation).build();
    }

    return basicJobInformation;
  }

  private String extractActuator() {
    return messageParser.readOrDefault(PRINCIPAL_EMAIL_PATH, EMPTY_STRING);
  }

  private String extractJobId() {
    return messageParser.readOrDefault(JOB_ID, EMPTY_STRING);
  }

  private Instant extractTimestamp() {
    String timestamp = messageParser.readOrDefault(TIMESTAMP, EMPTY_STRING);
    return isBlank(timestamp) ? Instant.EPOCH : Instant.parse(timestamp);
  }

  private String extractJobType() {
    return messageParser.readOrDefault(JOB_TYPE, EMPTY_STRING);
  }
}
