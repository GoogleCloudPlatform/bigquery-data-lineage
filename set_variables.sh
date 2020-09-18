#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export PROJECT_ID="<your-project-id>"
export REGION_ID="us-central1"
export AUDIT_LOGS_PUBSUB_TOPIC="bq-audit-logs"
export LOG_SINK_ID="bq_audit_logs_to_pubsub"
export LINEAGE_OUTPUT_PUBSUB_TOPIC="composite-lineage"
export BIGQUERY_REGION="us"
export DATASET_ID="audit_dataset"
export LINEAGE_TABLE_ID="Lineage"
export TEMP_GCS_BUCKET="<your-gcs-bucket>"
export LINEAGE_TAG_TEMPLATE_NAME="data_lineage_tag"
export LINEAGE_TAG_TEMPLATE_ID="projects/$PROJECT_ID/locations/$REGION_ID/tagTemplates/$LINEAGE_TAG_TEMPLATE_NAME"
export EXTRACTION_MAIN_CLASS="com.google.cloud.solutions.datalineage.LineageExtractionPipeline"
export PROPAGATION_MAIN_CLASS="com.google.cloud.solutions.datalineage.PolicyPropagationPipeline"
