package com.google.cloud.solutions.datalineage.extractor;

import static com.google.cloud.solutions.datalineage.converter.ResolvedColumnToColumnEntityConverter.convertToColumnEntity;

import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.util.HashMap;
import java.util.stream.IntStream;

public class InsertStatementExtractor extends ColumnLineageExtractor {

  public InsertStatementExtractor(ResolvedNodes.ResolvedStatement resolvedStatement) {
    super(resolvedStatement);
  }

  @Override
  public String getSupportedColumnType() {
    // TODO skuehn this PR determine how to support column types and the extractor instantiation
    //   optimizations
    return null;
  }

  @Override
  public ImmutableMap<ColumnEntity, ColumnLineage> extract() {
    HashMap<ColumnEntity, ColumnLineage> exprLineageMapBuilder = new HashMap<>();

    resolvedStatement.accept(
        new ResolvedNodes.Visitor() {
          @Override
          public void visit(ResolvedNodes.ResolvedInsertStmt insertStmt) {
            if (!insertStmt.getQueryOutputColumnList().isEmpty()) {
              // For inserts, BQ requires values to be added in the same order as the specified
              // columns, and the number of values added must match the number of specified columns.
              IntStream.range(0, insertStmt.getInsertColumnList().size()).forEach(columnIndex -> {
                ColumnEntity outputColumn =
                    convertToColumnEntity(insertStmt.getInsertColumnList().get(columnIndex));
                ResolvedColumn sourceColumn = insertStmt.getQueryOutputColumnList()
                    .get(columnIndex);

                exprLineageMapBuilder.put(
                    outputColumn,
                    ColumnLineage.newBuilder()
                        .setTarget(outputColumn)
                        .addAllParents(ImmutableList.of(convertToColumnEntity(sourceColumn)))
                        .build());
              });
            }
            super.visit(insertStmt);
          }
        });

    return ImmutableMap.copyOf(exprLineageMapBuilder);
  }
}
