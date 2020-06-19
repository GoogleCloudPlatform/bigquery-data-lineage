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

import com.google.cloud.solutions.datalineage.converter.MessageParser;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;

/**
 * A base extractor for Lineage information.
 * <p>Provides a thin wrapper for extracting metadata section of the JSON.</p>
 */
public abstract class LineageExtractor {

  protected static final String METADATA_PATH = "$.protoPayload.metadata";
  public static final String EMPTY_STRING = "";
  protected final JsonMessageParser messageParser;

  public LineageExtractor(String messageJson) {
    this(JsonMessageParser.of(messageJson));
  }

  public LineageExtractor(JsonMessageParser messageParser) {
    this.messageParser = messageParser;
  }

  /**
   * Returns the CompositeLineage with appropriate sections.
   */
  public abstract CompositeLineage extract();

  protected final MessageParser metadata() {
    return messageParser.forSubNode(METADATA_PATH);
  }
}