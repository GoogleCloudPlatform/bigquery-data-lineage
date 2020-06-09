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

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface LineageExtractionPipelineOptions extends PipelineOptions {

  /**
   * The fully  qualified table name of the BigQuery table to store lineage.
   */
  @Description(
      "Fully qualified BQ table name to store lineage.\nFormat:\nproject-id:dataset_id.table_id")
  @Validation.Required
  String getLineageTableName();

  void setLineageTableName(String lineageTableName);

  @Description("The name of topic to listen for audit-logs")
  @Validation.Required
  String getPubsubTopic();

  void setPubsubTopic(String pubsubTopic);

  @Description("The Data Catalog Tag Template Id registered for Lineage")
  @Validation.Required
  @Default.String(value = "")
  String getTagTemplateId();

  void setTagTemplateId(String tagTemplateId);

  @Description("Pub/Sub topic for publishing processed Lineage")
  @Validation.Required
  @Default.String(value = "")
  String getCompositeLineageTopic();

  void setCompositeLineageTopic(String pubsubTopic);

  @Description("List of BigQuery tables to omit from observation")
  List<String> getNonMonitoredTables();

  void setNonMonitoredTables(List<String> nonMonitoredTables);
}
