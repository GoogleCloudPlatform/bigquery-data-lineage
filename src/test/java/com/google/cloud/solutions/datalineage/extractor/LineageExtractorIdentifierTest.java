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

import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class LineageExtractorIdentifierTest {

  @Test
  public void identify_notInsertJob_noOpExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(
            TestResourceLoader.load("get_query_results_message.json"))
            .identify())
        .isInstanceOf(NoOpExtractor.class);
  }

  @Test
  public void identify_insertJob_insertJobExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(
            TestResourceLoader.load("complete_bq_last_message.json"))
            .identify())
        .isInstanceOf(InsertJobTableLineageExtractor.class);
  }

  @Test
  public void identify_jobError_noOpExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(TestResourceLoader.load("bq_ddl_statement_job_error.json"))
            .identify())
        .isInstanceOf(NoOpExtractor.class);
  }

  @Test
  public void identify_readTableOperation_noOpExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(
            TestResourceLoader.load("bq_insert_job_readTable_operation.json"))
            .identify())
        .isInstanceOf(NoOpExtractor.class);
  }

  @Test
  public void identify_nonLastMessage_noOpExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(
            TestResourceLoader.load("complete_bq_first_message.json"))
            .identify())
        .isInstanceOf(NoOpExtractor.class);
  }

  @Test
  public void identify_noMetadata_noOpExtractor() {
    assertThat(
        LineageExtractorIdentifier.of(
            TestResourceLoader.load("bq_message_without_metadata.json"))
            .identify())
        .isInstanceOf(NoOpExtractor.class);
  }

}
