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

package com.google.cloud.solutions.datalineage.model;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class CloudStorageFileTest {

  @Test
  public void create_valid() {

    assertThat(CloudStorageFile.create("gs://my-bucket/my-file.csv"))
        .isEqualTo(
            CloudStorageFile.builder()
                .setBucket("my-bucket")
                .setFile("my-file.csv")
                .build());
  }

  @Test
  public void create_invalidScheme_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> CloudStorageFile.create("ds://my-bucket/my-file.csv"));

    assertThat(exception).hasMessageThat()
        .isEqualTo(
            "fileName (ds://my-bucket/my-file.csv) needs to start with gs://<bucketName>/<filepath>");
  }

  @Test
  public void create_onlyBucket_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> CloudStorageFile.create("gs://my-bucket/"));

    assertThat(exception).hasMessageThat()
        .isEqualTo("fileName (gs://my-bucket/) needs to start with gs://<bucketName>/<filepath>");
  }
}