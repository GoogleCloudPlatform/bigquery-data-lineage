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

import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.common.collect.ImmutableSet;
import java.util.List;

/**
 * Extracts complete lineage for a BigQuery Copy Job.
 */
public final class CopyJobExtractor extends LineageExtractor {

  private static final String COPY_DESTINATION_TABLE = "$.jobChange.job.jobConfig.tableCopyConfig.destinationTable";
  private static final String COPY_SOURCE_TABLES = "$.jobChange.job.jobConfig.tableCopyConfig.sourceTables";

  public CopyJobExtractor(JsonMessageParser messageParser) {
    super(messageParser);
  }

  public CopyJobExtractor(String messageJson) {
    super(messageJson);
  }

  @Override
  public CompositeLineage extract() {

    BigQueryTableEntity self = BigQueryTableCreator
        .fromBigQueryResource(metadata().read(COPY_DESTINATION_TABLE));
    BigQueryTableEntity source = BigQueryTableCreator
        .fromBigQueryResource(metadata().<List<String>>read(COPY_SOURCE_TABLES).get(0));

    if (source.isTempTable() || self.isTempTable()) {
      return CompositeLineage.getDefaultInstance();
    }

    return
        CompositeLineage.newBuilder()
            .setTableLineage(
                TableLineage.newBuilder()
                    .setOperation("COPY_JOB")
                    .setTarget(self.dataEntity())
                    .addAllParents(ImmutableSet.of(source.dataEntity()))
                    .build()).build();
  }
}
