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

package com.google.cloud.solutions.datalineage.extractor;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.cloud.solutions.datalineage.converter.MessageParser;
import com.google.common.flogger.FluentLogger;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.concurrent.TimeUnit;

/**
 * A Parser for JSON using {@link JsonPath} library.
 */
public final class JsonMessageParser implements MessageParser {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final String messageJson;
  private final DocumentContext parsedMessage;

  public JsonMessageParser(String messageJson) {
    this.messageJson = messageJson;
    this.parsedMessage = JsonPath.parse(messageJson);
  }

  public static JsonMessageParser of(String messageJson) {
    return new JsonMessageParser(messageJson);
  }

  /**
   * Returns the JSON message used by this parser instant.
   */
  public String getJson() {
    return messageJson;
  }

  @Override
  public <T> T read(String jsonPath) {
    try {
      return parsedMessage.read(jsonPath);
    } catch (NullPointerException | PathNotFoundException exception) {
      logger.atInfo()
          .withCause(exception)
          .atMostEvery(1, TimeUnit.MINUTES)
          .log("error reading [%s]", jsonPath);
      return null;
    }
  }

  @Override
  public <T> T readOrDefault(String jsonPath, T defaultValue) {
    return firstNonNull(read(jsonPath), defaultValue);
  }

  @Override
  public boolean containsKey(String keyPath) {
    try {
      return (parsedMessage.read(keyPath) != null);
    } catch (PathNotFoundException e) {
      return false;
    }
  }
}