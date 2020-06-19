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

import static com.google.cloud.solutions.datalineage.testing.JsonAssert.assertJsonEquals;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import org.junit.Test;

public class JsonMessageParserTest {

  private static final String TEST_JSON = "{\n"
      + "  \"code\": 403,\n"
      + "  \"errors\": [\n"
      + "    {\n"
      + "      \"domain\": \"global\",\n"
      + "      \"message\": \"Access Denied\",\n"
      + "      \"reason\": \"accessDenied\"\n"
      + "    }\n"
      + "  ],\n"
      + "  \"status\": \"PERMISSION_DENIED\",\n"
      + "  \"keyWithNestedValues\": {\n"
      + "    \"nestedKey1\": \"nestedValue1\",\n"
      + "    \"secondLevelNestedKey\": {\n"
      + "      \"keyInSecond\": \"ValueInSecond\"\n"
      + "    }\n"
      + "  }\n"
      + "}";

  @Test
  public void getJson_completeJson() {
    assertJsonEquals(JsonMessageParser.of(TEST_JSON).getJson(), TEST_JSON);
  }

  @Test
  public void getRootPath_defaultRoot_equalsDollar() {
    assertThat(JsonMessageParser.of(TEST_JSON).getRootPath())
        .isEqualTo("$");
  }

  @Test
  public void getRootPath_nestedRoot_equalsCompleteRoot() {
    assertThat(JsonMessageParser.of(TEST_JSON)
        .forSubNode("$.keyWithNestedValues")
        .forSubNode("$.secondLevelNestedKey")
        .getRootPath())
        .isEqualTo("$.keyWithNestedValues.secondLevelNestedKey");
  }

  @Test
  public void read_emptyRootSingleStringValue_correct() {
    assertThat(JsonMessageParser.of(TEST_JSON).<String>read("$.status"))
        .isEqualTo("PERMISSION_DENIED");
  }

  @Test
  public void read_emptyRootSingleIntegerValue_correct() {
    assertThat(JsonMessageParser.of(TEST_JSON).<Integer>read("$.code")).isEqualTo(403);
  }

  @Test
  public void read_subNodeRootSingleValue_correct() {
    assertThat(
        JsonMessageParser.of(TEST_JSON)
            .forSubNode("$.keyWithNestedValues")
            .<String>read("$.nestedKey1"))
        .isEqualTo("nestedValue1");
  }

  @Test
  public void read_subNodeSecondLevelNestingRootSingleValue_correct() {
    assertThat(
        JsonMessageParser.of(TEST_JSON)
            .forSubNode("$.keyWithNestedValues")
            .forSubNode("$.secondLevelNestedKey")
            .<String>read("$.keyInSecond"))
        .isEqualTo("ValueInSecond");
  }

  @Test
  public void read_invalidKey_null() {
    assertThat(JsonMessageParser.of(TEST_JSON).<Object>read("$.missingKey")).isNull();
  }

  @Test
  public void readOrDefault_keyPresent_actualValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault("$.status", "DEFAULT"))
        .isEqualTo("PERMISSION_DENIED");
  }

  @Test
  public void readOrDefault_keyMissing_defaultValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault("$.status2", "DEFAULT"))
        .isEqualTo("DEFAULT");
  }

  @Test
  public void readOrDefault_nullKey_defaultValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault(null, "DEFAULT"))
        .isEqualTo("DEFAULT");
  }

  @Test
  public void containsKey_keyPresent_true() {
    assertThat(JsonMessageParser.of(TEST_JSON).containsKey("$.status"))
        .isTrue();
  }

  @Test
  public void containsKey_keyMissing_false() {
    assertThat(JsonMessageParser.of(TEST_JSON).containsKey("$.status2"))
        .isFalse();
  }

  @Test
  public void containsKey_keyNull_false() {
    assertThat(JsonMessageParser.of(TEST_JSON).containsKey(null))
        .isFalse();
  }

  @Test
  public void setMessageJson_null_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> JsonMessageParser.of(null));
  }

  @Test
  public void setMessageJson_empty_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> JsonMessageParser.of(""));
  }

  @Test
  public void setMessageJson_blank_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> JsonMessageParser.of("  "));
  }

  @Test
  public void equals_isSane() {
    assertThat(JsonMessageParser.of(TEST_JSON))
        .isEqualTo(JsonMessageParser.of(TEST_JSON));
    assertThat(JsonMessageParser.of(TEST_JSON))
        .isNotSameInstanceAs(JsonMessageParser.of(TEST_JSON));
    assertThat(JsonMessageParser.of(TEST_JSON))
        .isNotEqualTo(JsonMessageParser.of("{}"));
    assertThat(JsonMessageParser.of(TEST_JSON))
        .isNotEqualTo(
            JsonMessageParser.builder()
                .setMessageJson(TEST_JSON)
                .setRootPath("$.keyWithNestedValues")
                .build());
    assertThat(JsonMessageParser.of(TEST_JSON).equals(null)).isFalse();
    assertThat(JsonMessageParser.of(TEST_JSON).equals(new HashMap<>())).isFalse();

    JsonMessageParser testParser = JsonMessageParser.of(TEST_JSON);
    assertThat(testParser.equals(testParser)).isTrue();
  }

  @Test
  public void hashCode_isSane() {
    assertThat(JsonMessageParser.of(TEST_JSON).hashCode())
        .isNotEqualTo(JsonMessageParser.of(TEST_JSON).hashCode());
  }

}