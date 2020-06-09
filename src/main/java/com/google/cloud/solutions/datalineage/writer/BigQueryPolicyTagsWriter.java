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

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TargetPolicyTags;
import com.google.cloud.solutions.datalineage.service.BigQueryPolicyTagService;
import com.google.cloud.solutions.datalineage.service.BigQueryServiceFactory;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A Beam sink to update Policy Tags to BigQuery tables.
 */
@AutoValue
public abstract class BigQueryPolicyTagsWriter extends
    PTransform<PCollection<TargetPolicyTags>, PCollection<Void>> {

  abstract BigQueryServiceFactory bigQueryServiceFactory();

  @Override
  public PCollection<Void> expand(PCollection<TargetPolicyTags> input) {

    return input.apply("write policy tags", ParDo.of(
        new DoFn<TargetPolicyTags, Void>() {

          @ProcessElement
          public void applyAllTags(@Element TargetPolicyTags targetTags) throws IOException {
            BigQueryPolicyTagService.usingServiceFactory(bigQueryServiceFactory())
                .updatePoliciesForTable(
                    BigQueryTableCreator
                        .fromBigQueryResource(targetTags.getTable().getLinkedResource()))
                .withPolicies(targetTags.getPolicyTagsList())
                .apply();
          }

        }
    ));
  }

  public static Builder builder() {
    return new AutoValue_BigQueryPolicyTagsWriter.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder bigQueryServiceFactory(BigQueryServiceFactory bigQueryServiceFactory);

    public abstract BigQueryPolicyTagsWriter build();
  }
}
