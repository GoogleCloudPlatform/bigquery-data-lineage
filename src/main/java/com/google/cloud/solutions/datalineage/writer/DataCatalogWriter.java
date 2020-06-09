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

package com.google.cloud.solutions.datalineage.writer;

import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.common.flogger.FluentLogger;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Uses Data Catalog gRPC API to retrieve existing Tags to make a decision to update an existing
 * Lineage tag or create new.
 */
public final class DataCatalogWriter extends
    PTransform<PCollection<TagsForCatalog>, PCollection<Void>> {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  @Override
  public PCollection<Void> expand(PCollection<TagsForCatalog> input) {
    return input.apply("write to data catalog", ParDo.of(
        new DoFn<TagsForCatalog, Void>() {
          @ProcessElement
          public void processLineageTag(@Element TagsForCatalog tagsForCatalog) {
            try {
              DataCatalogService
                  .using(DataCatalogClient.create())
                  .applyEntityTags(tagsForCatalog.getEntryId(), tagsForCatalog.parsedTags());
            } catch (Exception exception) {
              logger.atWarning().every(100).withCause(exception)
                  .log("Error adding %s", tagsForCatalog);
            }
          }
        }
    ));
  }
}
