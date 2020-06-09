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

package com.google.cloud.solutions.datalineage.converter;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.FluentLogger;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utility methods to transform Java-beans to JSON using Jackson.
 */
public final class ProtoJsonConverter {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  /**
   * Returns the Object serialized as JSON using Jackson ObjectWriter or Protobuf JsonFormat if the
   * object is a Message.
   *
   * @param object the object to get JSON representation of.
   * @return JSON representation of the object or EMPTY string in case of error.
   */
  public static <T> String asJsonString(T object) {
    try {

      if (object == null) {
        return "";
      }

      if (object instanceof MessageOrBuilder) {
        return convertProtobufMessage((MessageOrBuilder) object);
      }

      if (object instanceof Collection) {
        return "[" +
            ((Collection<?>) object)
                .stream()
                .map(ProtoJsonConverter::asJsonString)
                .collect(Collectors.joining(","))
            + "]";
      }

      if (object instanceof Map) {
        return mapAsJsonString((Map<?, ?>) object);
      }

      return convertJavaBeans(object);

    } catch (IOException exp) {
      logger.atSevere().withCause(exp).log("Error in converting to Json");
    }

    return "";
  }

  /**
   * Returns a JSON string of the given Map.
   */
  private static String mapAsJsonString(Map<?, ?> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }

    return "{" +
        map.entrySet()
            .stream()
            .map(entry ->
                String.format("\"%s\": %s", entry.getKey(), asJsonString(entry.getValue())))
            .collect(Collectors.joining(","))
        + "}";
  }

  /**
   * Returns a JSON String representation using Jackson ObjectWriter.
   */
  private static <T> String convertJavaBeans(T object) throws JsonProcessingException {
    return new ObjectMapper().writer().writeValueAsString(object);
  }

  /**
   * Returns a JSON String representation using Protobuf JsonFormat.
   */
  public static String convertProtobufMessage(MessageOrBuilder message)
      throws InvalidProtocolBufferException {
    return JsonFormat.printer().print(message);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Message> T parseJson(String json, Class<T> protoClass) {
    try {
      Message.Builder builder =
          (Message.Builder)
              protoClass.getMethod("newBuilder")
                  .invoke(null);

      JsonFormat.parser().merge(json, builder);
      return (T) builder.build();
    } catch (Exception protoException) {
      logger.atSevere().withCause(protoException).atMostEvery(1, TimeUnit.MINUTES)
          .log("error converting json:\n%s", json);
      return null;
    }
  }

  public static <T extends Message> ImmutableList<T>
  parseAsList(Collection<String> jsons, Class<T> protoClass) {
    return jsons
        .stream()
        .map(json -> ProtoJsonConverter.parseJson(json, protoClass))
        .filter(Objects::nonNull)
        .collect(toImmutableList());
  }

  public static <T extends Message> ImmutableList<T>
  parseAsList(String jsonArray, Class<T> protoClass) {
    return parseAsList(
        JsonPath.parse(jsonArray)
            .<List<LinkedHashMap<String, Object>>>read("$")
            .stream()
            .map(ProtoJsonConverter::asJsonString)
            .collect(toImmutableList()),
        protoClass);
  }
}