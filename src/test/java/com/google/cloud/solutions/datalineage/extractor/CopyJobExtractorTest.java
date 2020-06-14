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

package com.google.cloud.solutions.datalineage.extractor;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CopyJobExtractorTest {

  @Test
  public void extract_copyJobTempTable_empty() {
    assertThat(
        new CopyJobExtractor(TestResourceLoader.load("bq_query_copy_temp_table.json"))
            .extract())
        .isEqualTo(CompositeLineage.getDefaultInstance());
  }

  @Test
  public void extract_copyJob_valid() {
    assertThat(
        new CopyJobExtractor(
            TestResourceLoader.load("bq_query_copy_table_valid.json"))
            .extract())
        .isEqualTo(
            CompositeLineage.newBuilder()
                .setTableLineage(TableLineage.newBuilder()
                    .setTarget(BigQueryTableCreator
                        .fromBigQueryResource(
                            "projects/helical-client-276602/datasets/airports/tables/airports_asia")
                        .dataEntity())
                    .addAllParents(ImmutableSet.of(
                        BigQueryTableCreator
                            .fromBigQueryResource(
                                "projects/helical-client-276602/datasets/my_dataset/tables/MySourceTable")
                            .dataEntity()
                    ))
                    .setOperation("COPY_JOB")
                    .build())
                .build());
  }

}
