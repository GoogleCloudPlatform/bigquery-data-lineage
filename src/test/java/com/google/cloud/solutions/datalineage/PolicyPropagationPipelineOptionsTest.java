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

import static com.google.common.truth.Truth.assertThat;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PolicyPropagationPipelineOptionsTest {

  @Test
  public void fromArgs_repeatedFieldSeparateLines_valid() {

    String[] args = new String[]{
        "--lineagePubSubTopic=projects/myproject1/topics/lineage-topic",
        "--monitoredCatalogTags=projects/myproject1/locations/us-central1/tagTemplates/pii_tag2",
        "--monitoredCatalogTags=projects/myproject1/locations/us-central1/tagTemplates/pii_tag"};

    PolicyPropagationPipelineOptions options
        = PipelineOptionsFactory
        .fromArgs(args)
        .as(PolicyPropagationPipelineOptions.class);

    assertThat(options.getLineagePubSubTopic())
        .isEqualTo("projects/myproject1/topics/lineage-topic");
    assertThat(options.getMonitoredCatalogTags())
        .containsExactly("projects/myproject1/locations/us-central1/tagTemplates/pii_tag2",
            "projects/myproject1/locations/us-central1/tagTemplates/pii_tag");
  }

  @Test
  public void fromArgs_repeatedFieldCommaSeparatedValues_valid() {

    String[] args = new String[]{
        "--lineagePubSubTopic=projects/myproject1/topics/lineage-topic",
        "--monitoredCatalogTags=projects/myproject1/locations/us-central1/tagTemplates/pii_tag2,"
            + "projects/myproject1/locations/us-central1/tagTemplates/pii_tag"};

    PolicyPropagationPipelineOptions options
        = PipelineOptionsFactory
        .fromArgs(args)
        .as(PolicyPropagationPipelineOptions.class);

    assertThat(options.getLineagePubSubTopic())
        .isEqualTo("projects/myproject1/topics/lineage-topic");
    assertThat(options.getMonitoredCatalogTags())
        .containsExactly("projects/myproject1/locations/us-central1/tagTemplates/pii_tag2",
            "projects/myproject1/locations/us-central1/tagTemplates/pii_tag");
  }

}
