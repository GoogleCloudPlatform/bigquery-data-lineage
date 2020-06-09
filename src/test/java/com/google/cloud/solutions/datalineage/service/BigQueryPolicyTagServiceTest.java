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

package com.google.cloud.solutions.datalineage.service;

import static com.google.cloud.solutions.datalineage.testing.GoogleTypesToJsonConverter.convertToJson;
import static com.google.cloud.solutions.datalineage.testing.JsonAssert.assertJsonEquals;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.Table;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnPolicyTags;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BigQueryPolicyTagServiceTest {

  @Before
  public void clearBigQueryTableLoadCache() {
    BigQueryTableLoadService.clearLocalCache();
  }

  @Test
  public void updatePoliciesForTable_updatesTableSchemaWithPolicyTag() {
    ImmutableList<ColumnPolicyTags> updatedTags =
        ImmutableList.of(
            ColumnPolicyTags.newBuilder()
                .setColumn("partner_name")
                .addPolicyTagIds(
                    "projects/GovernanceProject/locations/us/taxonomies/987654/policyTags/123456")
                .build(),
            ColumnPolicyTags.newBuilder()
                .setColumn("conversion_type")
                .addPolicyTagIds(
                    "projects/GovernanceProject/locations/us/taxonomies/987654/policyTags/789012")
                .build());

    Table updatedTable =
        BigQueryPolicyTagService.usingServiceFactory(
            FakeBigQueryServiceFactory
                .forTableSchemas(
                    TestResourceLoader
                        .load("schemas/CorePii_schema.json")))
            .updatePoliciesForTable(BigQueryTableEntity.create("demoProject", "demo", "CorePii"))
            .withPolicies(updatedTags)
            .apply();

    assertJsonEquals(convertToJson(updatedTable.getSchema().getFields()),
        "[\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"partner_phone_number\",\n"
            + "    \"type\": \"STRING\",\n"
            + "    \"policyTags\": {\n"
            + "      \"names\": [\n"
            + "        \"projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/1234\"\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"partner_id\",\n"
            + "    \"type\": \"INTEGER\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"partner_name\",\n"
            + "    \"type\": \"STRING\",\n"
            + "    \"policyTags\": {\n"
            + "      \"names\": [\n"
            + "        \"projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/7890\",\n"
            + "        \"projects/GovernanceProject/locations/us/taxonomies/987654/policyTags/123456\"\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"hit_timestamp\",\n"
            + "    \"type\": \"TIMESTAMP\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"conversion_type\",\n"
            + "    \"type\": \"STRING\",\n"
            + "    \"policyTags\": {\n"
            + "      \"names\": [\n"
            + "        \"projects/GovernanceProject/locations/us/taxonomies/987654/policyTags/789012\"\n"
            + "      ]\n"
            + "    }\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"num_products\",\n"
            + "    \"type\": \"INTEGER\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"num_hits\",\n"
            + "    \"type\": \"INTEGER\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"matched_tag_count\",\n"
            + "    \"type\": \"INTEGER\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"mode\": \"NULLABLE\",\n"
            + "    \"name\": \"is_matched_product\",\n"
            + "    \"type\": \"INTEGER\"\n"
            + "  }\n"
            + "]");
  }

  @Test
  public void readPolicies_noFilters_completePolicyTags() {
    ImmutableList<ColumnPolicyTags> policyTags =
        BigQueryPolicyTagService
            .usingServiceFactory(
                FakeBigQueryServiceFactory
                    .forTableSchemas(
                        TestResourceLoader.load("schemas/CorePii_schema.json")))
            .readPolicies(
                BigQueryTableEntity
                    .create("demoProject", "demo", "CorePii"));

    assertThat(policyTags)
        .containsExactly(
            ColumnPolicyTags.newBuilder()
                .setColumn("partner_phone_number")
                .addPolicyTagIds(
                    "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/1234")
                .build(),
            ColumnPolicyTags.newBuilder()
                .setColumn("partner_name")
                .addPolicyTagIds(
                    "projects/GovernanceProject/locations/us/taxonomies/8150274556907504807/policyTags/7890")
                .build());

  }
}