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

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.datalineage.converter.LineageToTagConverterFactory;
import com.google.cloud.solutions.datalineage.model.LineageMessages;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.common.flogger.FluentLogger;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Converts {@link LineageMessages.CompositeLineage} into a pair of Target Entity's DataCatalog
 * Entry Id and {@link Tag}s to apply.
 */
@AutoValue
public abstract class CompositeLineageToTagTransformation extends
    PTransform<PCollection<CompositeLineage>, PCollection<TagsForCatalog>> {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  public abstract String lineageTagTemplateId();

  @Nullable
  public abstract DataCatalogStub catalogStub();

  @Override
  public PCollection<TagsForCatalog> expand(PCollection<CompositeLineage> input) {
    return input.apply("convert to Data Catalog Tags", ParDo.of(
        new DoFn<CompositeLineage, TagsForCatalog>() {
          @ProcessElement
          public void convertToTag(
              @Element CompositeLineage lineage,
              OutputReceiver<TagsForCatalog> out) {

            try {
              DataEntity targetTable = lineage.getTableLineage().getTarget();

              DataCatalogService.usingStub(catalogStub())
                  .lookupEntry(targetTable)
                  .ifPresent(tableEntry ->
                      out.output(
                          TagsForCatalog
                              .forTags(LineageToTagConverterFactory
                                  .forTemplateId(lineageTagTemplateId())
                                  .converterFor(lineage)
                                  .buildTags())
                              .setEntryId(tableEntry.getName())
                              .build()));

            } catch (Exception exception) {
              logger.atWarning().withCause(exception)
                  .atMostEvery(1, TimeUnit.MINUTES)
                  .log("error converting CompositeLineage\n%s", lineage);
            }
          }
        }
    ));
  }

  public static CompositeLineageToTagTransformation withTagTemplateId(String tagTemplateId) {
    return builder().lineageTagTemplateId(tagTemplateId).build();
  }

  public static Builder builder() {
    return new AutoValue_CompositeLineageToTagTransformation.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder lineageTagTemplateId(String lineageTagTemplateId);

    public abstract Builder catalogStub(@Nullable DataCatalogStub catalogStub);

    public abstract CompositeLineageToTagTransformation build();
  }
}
