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

import static com.google.cloud.solutions.datalineage.testing.GoogleTypesToJsonConverter.convertFromJson;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.Table;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.FileDescriptorSetsBuilder;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.StructType;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BigQuerySchemaConverterTest {

  private static ImmutableList<?> convertToSerializedForm(SimpleColumn... columns) {
    return Arrays.stream(columns)
        .map(col -> col.serialize(new FileDescriptorSetsBuilder()))
        .collect(toImmutableList());
  }

  private static ImmutableList<?> convertToSerializedForm(Collection<SimpleColumn> columns) {
    return convertToSerializedForm(columns.toArray(new SimpleColumn[0]));
  }

  @Test
  public void convert_modelTypeTable_throwsException() {
    IllegalArgumentException exp =
        assertThrows(
            IllegalArgumentException.class, () ->
                BigQuerySchemaConverter.convert(new Table().setType("MODEL")));

    assertThat(exp).hasMessageThat().startsWith("Table Type should be \"TABLE\"");
  }

  @Test
  public void convert_simpleTypes_valid() {
    SimpleTable parsedTable =
        BigQuerySchemaConverter
            .convert(
                convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/bigquery_simple_type_table_schema.json")));

    String expectedTableName = "myproject.dataset.table";

    assertThat(parsedTable.getFullName()).isEqualTo(expectedTableName);
    assertThat(convertToSerializedForm(parsedTable.getColumnList()))
        .containsExactlyElementsIn(
            convertToSerializedForm(
                new SimpleColumn(
                    expectedTableName, "afloat", TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
                new SimpleColumn(
                    expectedTableName,
                    "aString",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                new SimpleColumn(
                    expectedTableName,
                    "aInteger",
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    expectedTableName, "aBool", TypeFactory.createSimpleType(TypeKind.TYPE_BOOL))))
        .inOrder();
  }

  @Test
  public void convert_allSimpleDataTypes_valid() {
    SimpleTable parsedTable =
        BigQuerySchemaConverter
            .convert(
                convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/bigquery_simple_all_types_table_schema.json")));

    String expectedTableName = "myproject.dataset.table";
    assertThat(parsedTable.getFullName()).isEqualTo(expectedTableName);
    assertThat(convertToSerializedForm(parsedTable.getColumnList()))
        .containsExactlyElementsIn(
            convertToSerializedForm(
                new SimpleColumn(
                    expectedTableName,
                    "afloat",
                    TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
                new SimpleColumn(
                    expectedTableName,
                    "aString",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                new SimpleColumn(
                    expectedTableName,
                    "aInteger",
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    expectedTableName,
                    "aBool",
                    TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
                new SimpleColumn(
                    expectedTableName,
                    "aBytes",
                    TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)),
                new SimpleColumn(
                    expectedTableName,
                    "aNumeric",
                    TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)),
                new SimpleColumn(
                    expectedTableName,
                    "aTimestamp",
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                new SimpleColumn(
                    expectedTableName,
                    "aDate",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATE)),
                new SimpleColumn(
                    expectedTableName,
                    "aTime",
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIME)),
                new SimpleColumn(
                    expectedTableName,
                    "aDateTime",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME)),
                new SimpleColumn(
                    expectedTableName,
                    "aGeoPoint",
                    TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY))
            ))
        .inOrder();
  }

  @Test
  public void convert_complexTable_valid() {
    SimpleTable parseTableSchema =
        BigQuerySchemaConverter
            .convert(
                convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/simple_daily_report_table_schema.json")));

    String expectedTableName = "myproject.reporting.daily_report";
    assertThat(parseTableSchema.getName()).isEqualTo(expectedTableName);
    assertThat(convertToSerializedForm(parseTableSchema.getColumnList()))
        .containsExactlyElementsIn(
            convertToSerializedForm(
                new SimpleColumn(
                    expectedTableName,
                    "hit_timestamp",
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                new SimpleColumn(
                    expectedTableName,
                    "partner_id",
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    expectedTableName,
                    "partner_name",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                new SimpleColumn(
                    expectedTableName,
                    "products",
                    TypeFactory.createArrayType(
                        TypeFactory.createStructType(
                            ImmutableSet.<StructType.StructField>builder()
                                .add(
                                    new StructType.StructField(
                                        "jsonError",
                                        TypeFactory.createSimpleType(TypeKind.TYPE_STRING)))
                                .add(
                                    new StructType.StructField(
                                        "product",
                                        TypeFactory.createStructType(
                                            ImmutableSet.<StructType.StructField>builder()
                                                .add(
                                                    new StructType.StructField(
                                                        "id",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .add(
                                                    new StructType.StructField(
                                                        "name",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .add(
                                                    new StructType.StructField(
                                                        "seller",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .add(
                                                    new StructType.StructField(
                                                        "quantity",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .add(
                                                    new StructType.StructField(
                                                        "value",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .add(
                                                    new StructType.StructField(
                                                        "currency",
                                                        TypeFactory.createSimpleType(
                                                            TypeKind.TYPE_STRING)))
                                                .build())))
                                .build()))),
                new SimpleColumn(
                    expectedTableName,
                    "latency",
                    TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
                new SimpleColumn(
                    expectedTableName, "is_ok", TypeFactory.createSimpleType(TypeKind.TYPE_BOOL))))
        .inOrder();
  }
}
