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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.common.collect.ImmutableMap;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;

/**
 * A base class to provide structure and common methods to Parsing a SQL statement using {@code
 * ZetaSQL} engine.
 */
public abstract class ColumnLineageExtractor {

  /**
   * Processed SQL statement used as a parser instance.
   */
  protected final ResolvedStatement resolvedStatement;

  /**
   * Builds an instance for Extraction of Column Lineage.
   *
   * @param resolvedStatement the processed SQL statement to be used as a parser.
   * @throws NullPointerException if resolvedStatement is null.
   */
  public ColumnLineageExtractor(ResolvedStatement resolvedStatement) {
    this.resolvedStatement = checkNotNull(resolvedStatement);
  }

  /**
   * The types of Processed Columns supported by this Extractor.
   */
  public abstract String getSupportedColumnType();

  /**
   * Returns a map of Column to Lineage information by parsing SQL.
   */
  public abstract ImmutableMap<ColumnEntity, ColumnLineage> extract();
}
