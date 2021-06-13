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

package com.google.cloud.solutions.datalineage.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/**
 * Value class to represent SQL Query's output columns.
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
@JsonDeserialize(builder = QueryColumns.Builder.class)
public abstract class QueryColumns {

  public abstract ImmutableMap<String, ColumnEntity> getColumnMap();

  @SchemaCreate
  public static QueryColumns create(ImmutableMap<String, ColumnEntity> columnMap) {
    return builder()
        .setColumnMap(columnMap)
        .build();
  }

  @JsonCreator
  public static Builder builder() {
    return new AutoValue_QueryColumns.Builder();
  }

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "set")
  public abstract static class Builder {

    public abstract Builder setColumnMap(ImmutableMap<String, ColumnEntity> outColumns);

    public abstract QueryColumns build();
  }
}