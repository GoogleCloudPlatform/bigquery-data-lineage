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

package com.google.cloud.solutions.datalineage.converter;

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseAsList;
import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseJson;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.cloud.solutions.datalineage.testing.FakeDataCatalogStub;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class LineageTagPropagationConverterFactoryTest {

  @Test
  public void processor_outputTableMissingInCatalog_empty() throws IOException {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json"),
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));
    LineageTagPropagationConverterFactory propagationConverterFactory =
        LineageTagPropagationConverterFactory.builder()
            .lineage(parseJson(TestResourceLoader.load(
                "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                CompositeLineage.class))
            .monitoredSourceTags(
                ImmutableList.of(
                    "projects/myproject1/locations/us-central1/tagTemplates/pii_tag",
                    "projects/myproject1/locations/us-central1/tagTemplates/pii_tag2"))
            .dataCatalogService(DataCatalogService.usingStub(fakeStub))
            .build();

    assertThat(propagationConverterFactory.processor().propagationTags())
        .isEqualTo(TagsForCatalog.empty());
  }

  @Test
  public void propagationTags_valid() throws IOException {
    FakeDataCatalogStub fakeStub = FakeDataCatalogStub.buildWithTestData(
        ImmutableList.of("datacatalog-objects/TableA_entry.json",
            "datacatalog-objects/simple_report_view_entry.json",
            "datacatalog-objects/OutputTable_entry.json"),
        ImmutableList.of(
            "datacatalog-objects/TableA_tags.json",
            "datacatalog-objects/simple_report_view_tags.json"));
    LineageTagPropagationConverterFactory propagationConverterFactory =
        LineageTagPropagationConverterFactory.builder()
            .lineage(parseJson(TestResourceLoader.load(
                "composite-lineages/complete_composite_lineage_tableA_simple_report_view_outputTable.json"),
                CompositeLineage.class))
            .monitoredSourceTags(
                ImmutableList.of(
                    "projects/myproject1/locations/us-central1/tagTemplates/pii_tag",
                    "projects/myproject1/locations/us-central1/tagTemplates/pii_tag2"))
            .dataCatalogService(DataCatalogService.usingStub(fakeStub))
            .build();

    assertThat(propagationConverterFactory.processor().propagationTags())
        .isEqualTo(
            TagsForCatalog
                .forTags(parseAsList("[{\n"
                    + "      \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag\",\n"
                    + "      \"fields\": {\n"
                    + "        \"type\": {\n"
                    + "          \"displayName\": \"PII Type\",\n"
                    + "          \"stringValue\": \"SENSITIVE\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"\"\n"
                    + "    }, {\n"
                    + "      \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag\",\n"
                    + "      \"fields\": {\n"
                    + "        \"type\": {\n"
                    + "          \"displayName\": \"PII Type\",\n"
                    + "          \"stringValue\": \"USER_ID\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"combined_telephone\"\n"
                    + "    }, {\n"
                    + "      \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag2\",\n"
                    + "      \"fields\": {\n"
                    + "        \"type\": {\n"
                    + "          \"displayName\": \"PII Type2\",\n"
                    + "          \"stringValue\": \"SENSITIVE\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"\"\n"
                    + "    }, {\n"
                    + "      \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag2\",\n"
                    + "      \"fields\": {\n"
                    + "        \"type\": {\n"
                    + "          \"displayName\": \"PII Type2\",\n"
                    + "          \"stringValue\": \"IMSI\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"combined_telephone\"\n"
                    + "    }, {\n"
                    + "      \"template\": \"projects/myproject1/locations/us-central1/tagTemplates/pii_tag2\",\n"
                    + "      \"fields\": {\n"
                    + "        \"type\": {\n"
                    + "          \"displayName\": \"PII Type2\",\n"
                    + "          \"stringValue\": \"IMSI\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      \"column\": \"telephone_number\"\n"
                    + "    }]", Tag.class))
                .setEntryId(
                    "projects/myproject1/locations/us/entryGroups/@bigquery/entries/OutputTableId")
                .build());
  }
}
