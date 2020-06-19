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

import static com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory.emptyLoaderFactory;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.extractor.InsertJobTableLineageExtractor;
import com.google.cloud.solutions.datalineage.extractor.JsonMessageParser;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoaderFactory;
import com.google.protobuf.Message;
import java.time.Clock;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class LineageExtractionTransform extends
    PTransform<PCollection<String>, PCollection<CompositeLineage>> {

  public abstract BigQueryTableEntity getOutputLineageTable();

  @Nullable
  public abstract Clock getClock();

  @Nullable
  public abstract ZetaSqlSchemaLoaderFactory getZetaSqlSchemaLoaderFactory();

  @Override
  public PCollection<CompositeLineage> expand(PCollection<String> auditLogMessages) {
    return auditLogMessages
        .apply("Validate Log Events", Filter.by(AuditLogValidator.create(getOutputLineageTable())))
        .apply("Extract Lineage", ParDo.of(new IdentifyAndExtract()))
        .apply("Validate Lineage", Filter.by(checkNonEmptyLineage()));
  }

  /**
   * Returns true if the {@link CompositeLineage} contains at least jobInformation and TableLineage
   * and the target table is not the Lineage Output BigQuery table.
   */
  private SerializableFunction<CompositeLineage, Boolean> checkNonEmptyLineage() {
    return input ->
        isNonEmptyMessage(input.getJobInformation()) &&
            isNonEmptyMessage(input.getTableLineage());
  }

  /**
   * Checks if the given Protobuf Message is non-null and not-default.
   *
   * @param message the Protobuf message to check
   * @return {@code true} if message is non-null and not-equal to defaultInstance()
   */
  private static boolean isNonEmptyMessage(Message message) {
    return message != null && !message.equals(message.getDefaultInstanceForType());
  }

  private class IdentifyAndExtract extends DoFn<String, CompositeLineage> {

    @ProcessElement
    public void extract(@Element String messageJson, OutputReceiver<CompositeLineage> out) {
      out.output(
          new InsertJobTableLineageExtractor(getClock(), JsonMessageParser.of(messageJson),
              getZetaSqlSchemaLoaderFactory())
              .extract());
    }
  }

  public static LineageExtractionTransform create(
      BigQueryTableEntity outputLineageTable,
      @Nullable Clock clock,
      @Nullable ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory) {
    return builder()
        .setOutputLineageTable(outputLineageTable)
        .setClock(clock)
        .setZetaSqlSchemaLoaderFactory(zetaSqlSchemaLoaderFactory)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_LineageExtractionTransform.Builder()
        .setZetaSqlSchemaLoaderFactory(emptyLoaderFactory());
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setOutputLineageTable(BigQueryTableEntity newLineageTable);

    public abstract Builder setClock(@Nullable Clock newClock);

    public abstract Builder setZetaSqlSchemaLoaderFactory(
        @Nullable ZetaSqlSchemaLoaderFactory zetaSqlSchemaLoaderFactory);

    abstract LineageExtractionTransform autoBuild();

    public LineageExtractionTransform build() {
      LineageExtractionTransform transformObj = autoBuild();

      if (transformObj.getClock() == null) {
        transformObj = transformObj.toBuilder().setClock(Clock.systemUTC()).build();
      }

      if (transformObj.getZetaSqlSchemaLoaderFactory() == null) {
        transformObj = transformObj.toBuilder()
            .setZetaSqlSchemaLoaderFactory(emptyLoaderFactory()).build();
      }

      return transformObj;
    }
  }
}
