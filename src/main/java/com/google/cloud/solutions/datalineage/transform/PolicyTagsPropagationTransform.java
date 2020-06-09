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

package com.google.cloud.solutions.datalineage.transform;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TargetPolicyTags;
import com.google.cloud.solutions.datalineage.service.BigQueryPolicyTagService;
import com.google.cloud.solutions.datalineage.service.BigQueryServiceFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.FluentLogger;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Identifies monitored policy tags that are applied on source tables' columns, de-duplicates Policy
 * tags and creates a mapping for Target Columns' tags.
 */
@AutoValue
public abstract class PolicyTagsPropagationTransform extends
    PTransform<PCollection<CompositeLineage>, PCollection<TargetPolicyTags>> {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  abstract List<String> monitoredPolicyTags();

  abstract BigQueryServiceFactory bigQueryServiceFactory();

  @Override
  public PCollection<TargetPolicyTags> expand(PCollection<CompositeLineage> input) {

    return
        input
            .apply("identify policy tags", ParDo.of(
                new DoFn<CompositeLineage, TargetPolicyTags>() {
                  @ProcessElement
                  public void identifyPolicyTags(
                      @Element CompositeLineage lineage,
                      OutputReceiver<TargetPolicyTags> out) {
                    BigQueryPolicyTagService
                        .usingServiceFactory(bigQueryServiceFactory())
                        .finderForLineage(lineage)
                        .forPolicies(monitoredPolicyTags())
                        .ifPresent(out::output);
                  }
                }
            ));
  }

  public static Builder builder() {
    return new AutoValue_PolicyTagsPropagationTransform.Builder()
        .monitoredPolicyTags(ImmutableList.of());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder monitoredPolicyTags(List<String> monitoredPolicyTags);

    public abstract Builder bigQueryServiceFactory(BigQueryServiceFactory bigQueryServiceFactory);

    public Builder withNullableMonitoredPolicyTags(List<String> monitoredPolicyTags) {
      if (monitoredPolicyTags == null) {
        return this;
      }

      return monitoredPolicyTags(ImmutableList.copyOf(monitoredPolicyTags));
    }

    public abstract PolicyTagsPropagationTransform build();
  }
}
