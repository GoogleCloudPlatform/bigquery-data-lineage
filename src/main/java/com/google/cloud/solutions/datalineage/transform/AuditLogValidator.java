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

package com.google.cloud.solutions.datalineage.transform;

import static com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator.fromBigQueryResource;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.extractor.JsonMessageParser;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Validates the audit log message is an InsertJob type by checking for presence of Json key {@code
 * protoPayload.metadata.jobChange}. It removes messages that are not last, or contain errors.
 */
@AutoValue
public abstract class AuditLogValidator implements SerializableFunction<String, Boolean> {

  public abstract BigQueryTableEntity getOutputLineageTable();

  @Override
  public Boolean apply(String input) {
    return new MessageValidator(input).isProcessableEvent();
  }

  private class MessageValidator {

    private static final String LAST_OPERATION_FLAG_PATH = "$.operation.last";
    private static final String METADATA_ROOT = "$.protoPayload.metadata";
    private static final String JOB_STATE_PATH = "$.protoPayload.metadata.jobChange.job.jobStatus.jobState";
    private static final String JOB_STATUS_ERRORS_PATH = "$.protoPayload.metadata.jobChange.job.jobStatus.errors";
    private static final String QUERY_DESTINATION_TABLE = "$.jobChange.job.jobConfig.*.destinationTable";

    private final JsonMessageParser parser;

    public MessageValidator(String messageJson) {
      this.parser = JsonMessageParser.of(messageJson);
    }

    public boolean isProcessableEvent() {
      return isJobSuccessful() && validDestinationTable();
    }

    private boolean isJobSuccessful() {
      return containsJobStatus() && isDone() && isErrorFree() && isLastOperation();
    }

    private boolean validDestinationTable() {
      
      List<String> destinationTableList = parser.forSubNode(METADATA_ROOT).<List<String>>read(QUERY_DESTINATION_TABLE);
      if(destinationTableList.size() <= 0) {
        return false;
      }
      
      BigQueryTableEntity destinationTable =
          fromBigQueryResource(destinationTableList.get(0));

      return !(destinationTable.isTempTable() || getOutputLineageTable().equals(destinationTable));
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

  public static AuditLogValidator create(BigQueryTableEntity newOutputLineageTable) {
    return builder()
        .setOutputLineageTable(newOutputLineageTable)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_AuditLogValidator.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setOutputLineageTable(BigQueryTableEntity newLineageTable);

    public abstract AuditLogValidator build();
  }
}
