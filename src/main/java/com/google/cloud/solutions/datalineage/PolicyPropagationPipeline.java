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

package com.google.cloud.solutions.datalineage;

import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.service.BigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.cloud.solutions.datalineage.transform.CatalogTagsPropagationTransform;
import com.google.cloud.solutions.datalineage.transform.PolicyTagsPropagationTransform;
import com.google.cloud.solutions.datalineage.writer.BigQueryPolicyTagsWriter;
import com.google.cloud.solutions.datalineage.writer.DataCatalogWriter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public final class PolicyPropagationPipeline {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(PolicyPropagationPipelineOptions.class);
    PolicyPropagationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(PolicyPropagationPipelineOptions.class);

    buildPipeline(Pipeline.create(options), options).run();
  }

  @VisibleForTesting
  static Pipeline buildPipeline(Pipeline pipeline, PolicyPropagationPipelineOptions options) {
    PCollection<CompositeLineage> compositeLineage =
        pipeline
            .apply("Read Composite Lineage",
                PubsubIO
                    .readProtos(CompositeLineage.class)
                    .fromTopic(options.getLineagePubSubTopic()));

    if (options.getMonitoredCatalogTags() != null && !options.getMonitoredCatalogTags().isEmpty()) {
      //Validate Monitored Catalog Tags
      DataCatalogService.validateTemplateIds(options.getMonitoredCatalogTags());

      compositeLineage
          .apply("Identify Catalog Tags for destination table & columns",
              CatalogTagsPropagationTransform
                  .forMonitoredTags(options.getMonitoredCatalogTags())
                  .build())
          .apply("write to DataCatalog", DataCatalogWriter.newWriter());
    }

    if (options.getMonitoredPolicyTags() != null && !options.getMonitoredPolicyTags().isEmpty()) {
      compositeLineage
          .apply("Identify Policy Tags for Destination Table",
              PolicyTagsPropagationTransform.builder()
                  .monitoredPolicyTags(options.getMonitoredPolicyTags())
                  .bigQueryServiceFactory(BigQueryServiceFactory.defaultFactory())
                  .build())
          .apply("Write Policy Tags to BigQuery",
              BigQueryPolicyTagsWriter.builder()
                  .bigQueryServiceFactory(BigQueryServiceFactory.defaultFactory()).build());
    }

    return pipeline;
  }
}
