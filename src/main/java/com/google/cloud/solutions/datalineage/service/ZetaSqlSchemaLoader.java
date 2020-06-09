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

package com.google.cloud.solutions.datalineage.service;

import com.google.common.collect.ImmutableSet;
import com.google.zetasql.SimpleTable;

/**
 * Generic Table Schema Loader for Database systems.
 */
public interface ZetaSqlSchemaLoader {

  /**
   * Loads the schema from source in the ZetaSql table format for all the provided table names.
   *
   * @param tableNames the names of the ZetaSql Tables
   * @return the table schema for all the given table names
   */
  ImmutableSet<SimpleTable> loadSchemas(ImmutableSet<String> tableNames);

  ImmutableSet<SimpleTable> loadSchemas(String... tableNames);
}
