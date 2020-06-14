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

package com.google.cloud.solutions.datalineage.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter;
import com.google.common.collect.ImmutableTable;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class CatalogTagsApplicatorTest {

  @Mock
  DataCatalogService mockDataCatalogService;

  @Before
  public void setUpCatalogServiceMock() {
    when(mockDataCatalogService.createTag(any(), any())).thenReturn(Tag.getDefaultInstance());
    when(mockDataCatalogService.updateTag(any(), any())).thenReturn(Tag.getDefaultInstance());
  }

  @Test
  public void apply_mixExistingAndNewTags_updatesExistingAndCreatesNewTags() throws IOException {
    when(mockDataCatalogService.lookUpTagsForTemplateIds(eq("testEntryId"), any()))
        .thenReturn(
            ImmutableTable
                .of("projects/myproject1/locations/us-central1/tagTemplates/pii_tag",
                    "combined_telephone",
                    ProtoJsonConverter.parseJson("{\n"
                        + "    \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag\",\n"
                        + "    \"fields\": {\n"
                        + "      \"type\": {\n"
                        + "        \"displayName\": \"PII Type\",\n"
                        + "        \"stringValue\": \"USER_ID\"\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"column\": \"combined_telephone\"\n"
                        + "  }", Tag.class)));

    CatalogTagsApplicator.builder()
        .withDataCatalogService(mockDataCatalogService)
        .forTags(
            ProtoJsonConverter.parseAsList(
                "[ {\n"
                    + "    \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag\",\n"
                    + "    \"fields\": {\n"
                    + "      \"type\": {\n"
                    + "        \"displayName\": \"PII Type\",\n"
                    + "        \"stringValue\": \"USER_ID\"\n"
                    + "      }\n"
                    + "    },\n"
                    + "    \"column\": \"combined_telephone\"\n"
                    + "  },\n"
                    + "  {\n"
                    + "    \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag2\",\n"
                    + "    \"fields\": {\n"
                    + "      \"type\": {\n"
                    + "        \"displayName\": \"PII Type2\",\n"
                    + "        \"stringValue\": \"SENSITIVE\"\n"
                    + "      }\n"
                    + "    },\n"
                    + "    \"column\": \"\"\n"
                    + "  }]", Tag.class
            )

        )
        .build()
        .apply("testEntryId");

    verify(mockDataCatalogService, times(1))
        .createTag(eq("testEntryId"), eq(ProtoJsonConverter.parseJson("  {\n"
            + "    \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag2\",\n"
            + "    \"fields\": {\n"
            + "      \"type\": {\n"
            + "        \"displayName\": \"PII Type2\",\n"
            + "        \"stringValue\": \"SENSITIVE\"\n"
            + "      }\n"
            + "    },\n"
            + "    \"column\": \"\"\n"
            + "  }", Tag.class)));
    verify(mockDataCatalogService, times(1))
        .updateTag(eq(ProtoJsonConverter.parseJson("{\n"
            + "    \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag\",\n"
            + "    \"fields\": {\n"
            + "      \"type\": {\n"
            + "        \"displayName\": \"PII Type\",\n"
            + "        \"stringValue\": \"USER_ID\"\n"
            + "      }\n"
            + "    },\n"
            + "    \"column\": \"combined_telephone\"\n"
            + "  }", Tag.class)), any());
  }
}
