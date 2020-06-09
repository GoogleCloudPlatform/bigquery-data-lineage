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

package com.google.cloud.solutions.datalineage.exception;

import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;

/**
 * Wrapped RunTime exception thrown from BigQuery operations.
 */
public class BigQueryOperationException extends RuntimeException {

  public BigQueryOperationException(BigQueryTableEntity table) {
    this(table, null);
  }

  public BigQueryOperationException(BigQueryTableEntity table, Throwable cause) {
    super(String.format("BigQuery Operation exception for\n%s", table), cause);
  }
}
