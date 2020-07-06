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
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.datalineage.converter.LineageTagPropagationConverterFactory;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Identifies monitored Catalog Tag Template's tags that are applied on source tables and/or
 * columns, de-duplicates applicable tags and creates a mapping for Target Columns' tags.
 */
@AutoValue
public abstract class CatalogTagsPropagationTransform
    extends PTransform<PCollection<CompositeLineage>, PCollection<TagsForCatalog>> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public abstract ImmutableList<String> monitoredSourceTags();

  @Nullable
  public abstract DataCatalogStub catalogStub();


  @Override
  public PCollection<TagsForCatalog> expand(PCollection<CompositeLineage> input) {
    return input.apply("Identify Source Tags", ParDo.of(
        new DoFn<CompositeLineage, TagsForCatalog>() {
          @ProcessElement
          public void convertToTags(
              @Element CompositeLineage lineage,
              OutputReceiver<TagsForCatalog> out) {
            try {
              TagsForCatalog tags = LineageTagPropagationConverterFactory.builder()
                  .lineage(lineage)
                  .monitoredSourceTags(monitoredSourceTags())
                  .dataCatalogService(DataCatalogService.usingStub(catalogStub()))
                  .build()
                  .processor()
                  .propagationTags();
              if (!tags.getTagsJson().isEmpty()) {
                out.output(tags);
              }
            } catch (Exception exception) {
              logger.atWarning().withCause(exception)
                  .atMostEvery(1, TimeUnit.MINUTES)
                  .log("error expanding source tags for lineage:\n%s", lineage);
            }
          }


        }
    ));
  }

  public static CatalogTagsPropagationTransform.Builder forMonitoredTags(
      Collection<String> newMonitoredSourceTags) {

    if (newMonitoredSourceTags != null) {
      return builder().monitoredSourceTags(ImmutableList.copyOf(newMonitoredSourceTags));
    }

    return builder();
  }

  public static Builder builder() {
    return new AutoValue_CatalogTagsPropagationTransform.Builder()
        .monitoredSourceTags(ImmutableList.of());
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder monitoredSourceTags(ImmutableList<String> newMonitoredSourceTags);

    public abstract Builder catalogStub(@Nullable DataCatalogStub catalogStub);

    public abstract CatalogTagsPropagationTransform build();
  }
}
