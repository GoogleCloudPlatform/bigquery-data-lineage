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

package com.google.cloud.solutions.datalineage;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.solutions.datalineage.extractor.ColumnLineageExtractor;
import com.google.cloud.solutions.datalineage.extractor.ColumnLineageExtractorFactory;
import com.google.cloud.solutions.datalineage.extractor.FunctionExpressionsExtractor;
import com.google.cloud.solutions.datalineage.extractor.GroupByExtractor;
import com.google.cloud.solutions.datalineage.extractor.SimpleAggregateExtractor;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.QueryColumns;
import com.google.cloud.solutions.datalineage.service.ZetaSqlSchemaLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Simple SQL parser to identify lineage relations using ZetaSQL engine.
 */
public class BigQuerySqlParser {

  static {
    ColumnLineageExtractorFactory.register(
        FunctionExpressionsExtractor.class, GroupByExtractor.class,
        SimpleAggregateExtractor.class);
  }

  private final ZetaSqlSchemaLoader tableSchemaLoader;

  public BigQuerySqlParser(@Nullable ZetaSqlSchemaLoader tableSchemaLoader) {
    this.tableSchemaLoader = tableSchemaLoader;
  }

  public ImmutableSet<ColumnLineage> extractColumnLineage(String sql) {

    if (tableSchemaLoader == null) {
      return ImmutableSet.of();
    }

    ColumnLineageExtractorFactory columnLineageExtractorFactory =
        ColumnLineageExtractorFactory.forStatement(resolve(sql));

    return combine(columnLineageExtractorFactory.outputColumns(),
        extractLineages(columnLineageExtractorFactory.buildExtractors()));
  }

  /**
   * Combines output query Column names with their lineage information.
   *
   * @param queryColumns the output query columns
   * @param lineageParts lineage information for complex operation output columns.
   * @return a combined lookup of column names and lineage component.
   */
  private ImmutableSet<ColumnLineage> combine(
      QueryColumns queryColumns,
      ImmutableSet<ImmutableMap<ColumnEntity, ColumnLineage>> lineageParts) {

    return queryColumns.getColumnMap().entrySet().stream()
        .map(
            entry -> {
              ColumnEntity oColumn = ColumnEntity.newBuilder().setColumn(entry.getKey()).build();

              for (ImmutableMap<ColumnEntity, ColumnLineage> lineageMap : lineageParts) {
                if (lineageMap.containsKey(entry.getValue())) {
                  return lineageMap.get(entry.getValue())
                      .toBuilder().setTarget(oColumn).build();
                }
              }

              return ColumnLineage.newBuilder()
                  .setTarget(oColumn)
                  .addAllParents(ImmutableSet.of(entry.getValue()))
                  .build();
            })
        .collect(toImmutableSet());
  }

  private ImmutableSet<ImmutableMap<ColumnEntity, ColumnLineage>> extractLineages(
      ImmutableSet<ColumnLineageExtractor> extractors) {
    return extractors.stream()
        .map(ColumnLineageExtractor::extract)
        .filter(Objects::nonNull)
        .collect(toImmutableSet());
  }

  private static ImmutableSet<String> extractReferencedTables(String sql) {
    return Analyzer.extractTableNamesFromStatement(sql, enableAllFeatures()).stream()
        .flatMap(List::stream)
        .collect(toImmutableSet());
  }

  private ResolvedStatement resolve(String sql) {
    return Analyzer.analyzeStatement(sql, enableAllFeatures(), buildCatalogWithQueryTables(sql));
  }

  private static AnalyzerOptions enableAllFeatures() {
    LanguageOptions languageOptions = new LanguageOptions().enableMaximumLanguageFeatures();
    languageOptions.setSupportsAllStatementKinds();
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    analyzerOptions.setLanguageOptions(languageOptions);
    analyzerOptions.setPruneUnusedColumns(true);

    return analyzerOptions;
  }

  /**
   * Creates a ZetaSQL Catalog instance with Table schema (for referenced tables) loaded using the
   * provided SchemaLoader.
   *
   * @param sql the SQL Statement to load referenced tables schemas for.
   */
  private SimpleCatalog buildCatalogWithQueryTables(String sql) {
    SimpleCatalog catalog = new SimpleCatalog("queryCatalog");
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    if (tableSchemaLoader != null) {
      tableSchemaLoader.loadSchemas(extractReferencedTables(sql))
          .forEach(catalog::addSimpleTable);
    }

    return catalog;
  }
}
