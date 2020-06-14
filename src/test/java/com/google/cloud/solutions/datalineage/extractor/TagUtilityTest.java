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

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseAsList;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TagUtilityTest {

  @Test
  public void extractParent_valid() {
    List<Tag> tags = parseAsList(TestResourceLoader.load("datacatalog-objects/TableA_tags.json"),
        Tag.class);

    assertThat(TagUtility.extractParent(tags.get(0)))
        .isEqualTo(
            "projects/myproject1/locations/us/entryGroups/@bigquery/entries/TableAId");

  }
}
