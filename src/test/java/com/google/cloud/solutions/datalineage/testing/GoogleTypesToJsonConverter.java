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

package com.google.cloud.solutions.datalineage.testing;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class GoogleTypesToJsonConverter {

  public static <T> T convertFromJson(Class<T> clazz, String typeJson) {
    try (JsonParser parser = new GsonFactory().createJsonParser(typeJson)) {
      return parser.parse(clazz);
    } catch (IOException jsonProcessingException) {
      return null;
    }
  }

  public static <T> ImmutableList<T> convertFromJson(Class<T> clazz, String... typeJsons) {
    return convertFromJson(clazz, Arrays.asList(typeJsons));
  }

  public static <T> ImmutableList<T> convertFromJson(Class<T> clazz, Collection<?> typeJsons) {
    return typeJsons.stream()
        .map(GoogleTypesToJsonConverter::convertToJson)
        .map(item -> convertFromJson(clazz, item))
        .filter(Objects::nonNull)
        .collect(toImmutableList());
  }

  public static <T> String convertToJson(T item) {
    if (item instanceof String) {
      return item.toString();
    }

    StringWriter stringWriter = new StringWriter();
    try (JsonGenerator jsonGenerator = new GsonFactory().createJsonGenerator(stringWriter)) {
      // Output is primarily used for testing and hence enable pretty printing.
      jsonGenerator.enablePrettyPrint();
      jsonGenerator.serialize(item);
      jsonGenerator.flush();
      return stringWriter.toString();
    } catch (IOException ioException) {
      return "";
    }
  }

  private GoogleTypesToJsonConverter() {
  }
}
