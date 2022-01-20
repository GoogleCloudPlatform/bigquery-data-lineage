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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fake implementation of Bigquery client for testing. Does not implement batch() methods.
 */
public class FakeBigquery extends Bigquery {

  public final Map<String, Table> predefinedTables;
  @VisibleForTesting
  public final AtomicInteger numTimesCounterTables;
  @VisibleForTesting
  public final AtomicInteger numTimesCounterTablesGetExecute;

  public static FakeBigquery forTableSchemas(String... predefinedTableSchemas) {
    ImmutableMap<String, Table> predefinedTables =
        GoogleTypesToJsonConverter
            .convertFromJson(Table.class, predefinedTableSchemas)
            .stream()
            .collect(toImmutableMap(Table::getId, identity()));

    return new FakeBigquery(predefinedTables);
  }

  public FakeBigquery(Map<String, Table> predefinedTables) {
    super(new MockHttpTransport(), new GsonFactory(), null);
    this.predefinedTables = new HashMap<>(predefinedTables);
    this.numTimesCounterTables = new AtomicInteger(0);
    this.numTimesCounterTablesGetExecute = new AtomicInteger(0);
  }


  @Override
  public Tables tables() {

    class FakeTables extends Bigquery.Tables {

      @Override
      public Get get(String projectId, String datasetId, String tableId) {
        return new Tables.Get(projectId, datasetId, tableId) {
          @Override
          public Table execute() throws IOException {
            numTimesCounterTablesGetExecute.incrementAndGet();

            return safeTableGet(projectId, datasetId, tableId);
          }
        };
      }

      @Override
      public Patch patch(String projectId, String datasetId, String tableId, Table content) {

        return new Tables.Patch(projectId, datasetId, tableId, content) {
          @Override
          public Table execute() throws IOException {
            Table table = safeTableGet(projectId, datasetId, tableId);
            validateFields(table.getSchema().getFields(), content.getSchema().getFields());
            table.getSchema().setFields(content.getSchema().getFields());
            return table;
          }

          private void validateFields(java.util.List<TableFieldSchema> existingFields,
              java.util.List<TableFieldSchema> updatedFields) throws IOException {
            Map<String, TableFieldSchema> existingFieldMap = convertFieldListToMap(existingFields);
            Map<String, TableFieldSchema> updatedFieldMap = convertFieldListToMap(updatedFields);

            for (Entry<String, TableFieldSchema> entry : existingFieldMap.entrySet()) {
              TableFieldSchema existingField = entry.getValue();
              TableFieldSchema updatedField = updatedFieldMap.get(entry.getKey());
              checkFieldMatches(existingField, updatedField);

              if (existingField.getType().equals("RECORD")) {
                validateFields(existingField.getFields(), updatedField.getFields());
              }
            }
          }

          private ImmutableMap<String, TableFieldSchema> convertFieldListToMap(
              java.util.List<TableFieldSchema> fields) {
            return fields.stream().collect(toImmutableMap(TableFieldSchema::getName, identity()));
          }

          private void checkFieldMatches(
              TableFieldSchema existingField,
              TableFieldSchema updatedField) throws IOException {
            boolean matches =
                (existingField != null)
                    && (updatedField != null)
                    && Objects.equals(existingField.getName(), updatedField.getName())
                    && Objects.equals(existingField.getType(), updatedField.getType())
                    && Objects.equals(existingField.getMode(), updatedField.getMode());

            if (!matches) {
              String fieldName = (existingField == null) ? "null" : existingField.getName();
              throw new IOException("Field does not match: " + fieldName);
            }
          }
        };
      }
    }

    numTimesCounterTables.incrementAndGet();
    return new FakeTables();
  }

  private Table safeTableGet(String projectId, String datasetId, String tableId)
      throws IOException {
    BigQueryTableEntity tableEntity = BigQueryTableEntity.create(projectId, datasetId, tableId);
    Table table = predefinedTables.get(tableEntity.getLegacySqlName());

    if (table == null) {
      throw new IOException(
          String.format("Table Not Found %s:%s.%s", projectId, datasetId, tableId));
    }
    return table;
  }

  //TODO: Implement batch request tracking using MockTransport.
}
