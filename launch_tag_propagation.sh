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

mvn clean generate-sources compile package exec:java \
  -Dexec.mainClass=$PROPAGATION_MAIN_CLASS \
  -Dexec.cleanupDaemonThreads=false \
  -Dmaven.test.skip=true \
  -Dexec.args=" \
--streaming=true \
--project=$PROJECT_ID \
--runner=DataflowRunner \
--gcpTempLocation=gs://$TEMP_GCS_BUCKET/temp/ \
--stagingLocation=gs://$TEMP_GCS_BUCKET/staging/ \
--workerMachineType=n1-standard-1 \
--region=$REGION_ID \
--lineagePubSubTopic=projects/$PROJECT_ID/topics/$LINEAGE_OUTPUT_PUBSUB_TOPIC \
--monitoredCatalogTags=projects/bq-lineage-demo/locations/us-central1/tagTemplates/pii_tag \
--monitoredCatalogTags=projects/bq-lineage-demo/locations/us-central1/tagTemplates/pii_sensitivity \
--monitoredPolicyTags=projects/bq-lineage-demo/locations/us/taxonomies/544279842572406327/policyTags/2123206183673327057"
