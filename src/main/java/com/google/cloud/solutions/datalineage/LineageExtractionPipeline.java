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

package com.google.cloud.solutions.datalineage;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.solutions.datalineage.converter.CompositeLineageTableRowMapper;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.service.BigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.service.BigQueryZetaSqlSchemaLoaderFactory;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.cloud.solutions.datalineage.transform.CompositeLineageToTagTransformation;
import com.google.cloud.solutions.datalineage.transform.LineageExtractionTransform;
import com.google.cloud.solutions.datalineage.writer.DataCatalogWriter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LineageExtractionPipeline {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(LineageExtractionPipelineOptions.class);
    LineageExtractionPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(LineageExtractionPipelineOptions.class);

    buildPipeline(Pipeline.create(options), options).run();
  }

  @VisibleForTesting
  static Pipeline buildPipeline(Pipeline pipeline, LineageExtractionPipelineOptions options) {
    // Validate Tag Template Id
    DataCatalogService.validateTemplateId(options.getTagTemplateId());

    PCollection<CompositeLineage> extractedLineage =
        pipeline
            .apply("Retrieve Audit logs from PubSub",
                PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))
            .apply("Parse Message and extract Lineage",
                LineageExtractionTransform.builder()
                    .setOutputLineageTable(BigQueryTableCreator
                        .fromLegacyTableName(options.getLineageTableName()))
                    .setZetaSqlSchemaLoaderFactory(
                        BigQueryZetaSqlSchemaLoaderFactory
                            .usingServiceFactory(BigQueryServiceFactory.defaultFactory()))
                    .build());

    extractedLineage
        .apply("Store in BigQuery",
            BigQueryIO
                .<CompositeLineage>write()
                .to(options.getLineageTableName())
                .withTimePartitioning(
                    new TimePartitioning().setType("DAY").setField("lineageReconcileTime"))
                .withFormatFunction(CompositeLineageTableRowMapper.newMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // Store in Data Catalog if tag Template Id is provided.
    if (isNotBlank(options.getTagTemplateId())) {
      extractedLineage
          .apply("convert lineage to tags",
              CompositeLineageToTagTransformation.withTagTemplateId(options.getTagTemplateId()))
          .apply("Store in DataCatalog", DataCatalogWriter.newWriter());
    }

    // If enabled, output composite lineage to a pubsub topic for downstream processing.
    if (isNotBlank(options.getCompositeLineageTopic())) {
      extractedLineage
          .apply("Enable Downstream Use-Cases",
              PubsubIO
                  .writeProtos(CompositeLineage.class)
                  .to(options.getCompositeLineageTopic())
                  .withTimestampAttribute("reconcileTime"));
    }

    return pipeline;
  }
}
