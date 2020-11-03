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

import com.google.cloud.solutions.datalineage.model.CloudStorageFile;
import com.google.cloud.solutions.datalineage.model.DataEntityConvertible;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.common.collect.ImmutableSet;
import java.util.List;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/** Extracts complete lineage for a BigQuery Load Job. */
public final class LoadJobExtractor extends LineageExtractor {

  private static final String LOAD_DESTINATION_TABLE =
      "$.jobChange.job.jobConfig.loadConfig.destinationTable";
  private static final String LOAD_SOURCE_TABLES =
      "$.jobChange.job.jobConfig.loadConfig.sourceUris";

  public LoadJobExtractor(JsonMessageParser messageParser) {
    super(messageParser);
  }

  public LoadJobExtractor(String messageJson) {
    super(messageJson);
  }

  @Override
  public CompositeLineage extract() {
    return CompositeLineage.newBuilder()
        .setTableLineage(
            TableLineage.newBuilder()
                .setOperation("LOAD_JOB")
                .setTarget(
                    BigQueryTableCreator.fromBigQueryResource(
                            metadata().read(LOAD_DESTINATION_TABLE))
                        .dataEntity())
                .addAllParents(extractSources())
                .build())
        .build();
  }

  private ImmutableSet<DataEntity> extractSources() {
    List<String> sourceUris = metadata().read(LOAD_SOURCE_TABLES);

    if (sourceUris != null) {
      return
        sourceUris.stream()
          .map(CloudStorageFile::create)
          .map(DataEntityConvertible::dataEntity)
          .collect(toImmutableSet());
    }

    return ImmutableSet.of();
  }
}
