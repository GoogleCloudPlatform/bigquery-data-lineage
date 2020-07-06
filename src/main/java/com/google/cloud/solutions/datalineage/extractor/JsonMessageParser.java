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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.cloud.solutions.datalineage.converter.MessageParser;
import com.google.common.base.Objects;
import com.google.common.flogger.GoogleLogger;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.concurrent.TimeUnit;

/**
 * A Parser for JSON using {@link JsonPath} library.
 */
public final class JsonMessageParser implements MessageParser {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String messageJson;
  private final DocumentContext parsedMessage;
  private final String rootPath;

  private JsonMessageParser(String messageJson, String rootPath) {
    this(JsonPath.parse(messageJson), rootPath);
  }

  private JsonMessageParser(DocumentContext parsedMessage, String rootPath) {
    this.messageJson = parsedMessage.jsonString();
    this.parsedMessage = parsedMessage;
    this.rootPath = rootPath;
  }

  /**
   * Returns a JSON parser for a subnode of the present Object.
   */
  public JsonMessageParser forSubNode(String subNodeKey) {
    return new JsonMessageParser(parsedMessage, buildPath(subNodeKey));
  }

  public static JsonMessageParser of(String messageJson) {
    return builder().setMessageJson(messageJson).build();
  }

  /**
   * Returns the JSON message used by this parser instant.
   */
  public String getJson() {
    return parsedMessage.jsonString();
  }

  public String getRootPath() {
    return rootPath;
  }

  @Override
  public <T> T read(String jsonPath) {
    try {
      return parsedMessage.read(buildPath(jsonPath));
    } catch (PathNotFoundException | NullPointerException exception) {
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
      return (parsedMessage.read(buildPath(keyPath)) != null);
    } catch (PathNotFoundException | NullPointerException e) {
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JsonMessageParser)) {
      return false;
    }
    JsonMessageParser that = (JsonMessageParser) o;
    return Objects.equal(messageJson, that.messageJson) &&
        Objects.equal(rootPath, that.rootPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(parsedMessage, rootPath);
  }

  private String buildPath(String path) {
    checkArgument(path.startsWith("$."));
    return rootPath + "." + path.replaceFirst("^\\$\\.", "");
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Convenience Builder class for Json Parser.
   */
  public static class Builder {

    private String messageJson;
    private String rootPath = "$";

    public Builder setMessageJson(String messageJson) {
      this.messageJson = checkString(messageJson);
      return this;
    }

    public Builder setRootPath(String rootPath) {
      this.rootPath = checkString(rootPath);
      return this;
    }

    private static String checkString(String string) {
      if (isBlank(string)) {
        throw new NullPointerException("can't be null or empty (was: \"" + string + "\")");
      }

      return string;
    }

    public JsonMessageParser build() {
      return new JsonMessageParser(messageJson, rootPath);
    }
  }
}