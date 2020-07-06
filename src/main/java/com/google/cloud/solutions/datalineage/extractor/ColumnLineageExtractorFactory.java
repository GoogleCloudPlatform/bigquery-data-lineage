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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.solutions.datalineage.model.QueryColumns;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * A Factory to build instances of ColumnLineageExtractors for a given column type. It provides
 * methods like register to enable specific ExtractorTypes.
 */
public final class ColumnLineageExtractorFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Statement to be used for instantiating {@link ColumnLineageExtractor} during registration.
   */
  private static final ResolvedStatement EMPTY_RESOLVED_STATEMENT =
      Analyzer.analyzeStatement("SELECT 1", new AnalyzerOptions(), new SimpleCatalog("cat"));

  /**
   * Keeps a Map of ColumnTypes to Extractors.
   */
  private static final HashMultimap<String, Class<? extends ColumnLineageExtractor>> extractorTypeMap =
      HashMultimap.create();

  /**
   * The ZetaSQL processed SQL statement.
   */
  private final ResolvedStatement resolvedStatement;

  public ColumnLineageExtractorFactory(ResolvedStatement resolvedStatement) {
    this.resolvedStatement = resolvedStatement;
  }

  public static ColumnLineageExtractorFactory forStatement(ResolvedStatement statement) {
    return new ColumnLineageExtractorFactory(statement);
  }

  /**
   * Registers the {@link ColumnLineageExtractor} by adding to the mapping table.
   * <p> Instantiates a temporary instance of the Extractor and invokes {@link
   * ColumnLineageExtractor#getSupportedColumnType()} and adds the class to the ColumnType to
   * Extractor map.
   *
   * @param extractorClasses the Extractor classes to allow being used with the factory.
   */
  @SafeVarargs
  public static void register(Class<? extends ColumnLineageExtractor>... extractorClasses) {
    Arrays.stream(extractorClasses)
        .map(ColumnLineageExtractorFactory::buildExtractor)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            extractor ->
                extractorTypeMap.put(extractor.getSupportedColumnType(),
                    extractor.getClass()));
  }

  /**
   * Returns all output QueryColumns by parsing SQL using {@link OutputColumnExtractor}.
   */
  public QueryColumns outputColumns() {
    return new OutputColumnExtractor(resolvedStatement).extract();
  }

  /**
   * Returns a set of applicable Extractors for the provided SQL query.
   */
  public ImmutableSet<ColumnLineageExtractor> buildExtractors() {
    return outputColumns()
        .getProcessedColumnTypes().stream()
        .map(this::buildExtractorFor)
        .flatMap(Collection::stream)
        .distinct()
        .collect(toImmutableSet());
  }

  /**
   * Returns a set of extractors for given columnType or empty set, if not find.
   */
  public ImmutableSet<ColumnLineageExtractor> buildExtractorFor(String columnType) {
    return extractorTypeMap.get(columnType).stream()
        .map(clazz -> buildExtractor(clazz, resolvedStatement))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toImmutableSet());
  }

  private static Optional<ColumnLineageExtractor> buildExtractor(
      Class<? extends ColumnLineageExtractor> clazz, ResolvedStatement statement) {
    try {
      return Optional
          .of(clazz.getConstructor(ResolvedStatement.class).newInstance(statement));
    } catch (ReflectiveOperationException reflectiveOperationException) {
      logger.atWarning().withCause(reflectiveOperationException).log("error creating ");
    }
    return Optional.empty();
  }

  private static Optional<ColumnLineageExtractor> buildExtractor(
      Class<? extends ColumnLineageExtractor> clazz) {
    return buildExtractor(clazz, EMPTY_RESOLVED_STATEMENT);
  }
}