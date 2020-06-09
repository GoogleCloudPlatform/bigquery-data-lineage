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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.datalineage.exception.BigQueryOperationException;
import com.google.cloud.solutions.datalineage.testing.FakeBigQueryServiceFactory;
import com.google.cloud.solutions.datalineage.testing.FakeBigquery;
import com.google.cloud.solutions.datalineage.testing.TestResourceLoader;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BigQueryTableLoadServiceTest {

  @Before
  public void clearServiceCache() {
    BigQueryTableLoadService.clearLocalCache();
  }

  @Test
  public void loadTable_bigQueryEntity_loads() {
    BigQueryTableLoadService BigQueryTableLoadService =
        new BigQueryTableLoadService(
            FakeBigQueryServiceFactory.forTableSchemas(
                TestResourceLoader.load("schemas/tableA_schema.json")));

    Table loadedTable = BigQueryTableLoadService.loadTable("project1.datasetA.TableA");
    assertThat(loadedTable.getSchema())
        .isEqualTo(
            new TableSchema()
                .setFields(
                    ImmutableList.of(
                        new TableFieldSchema()
                            .setName("colA")
                            .setType("STRING")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("colC")
                            .setType("STRING")
                            .setMode("NULLABLE"))));
  }

  @Test
  public void loadTable_bigQueryEntityLoadTwoTimes_returnsCachedEntity() {
    FakeBigquery fakeBigquery =
        FakeBigquery.forTableSchemas(
            TestResourceLoader.load("schemas/tableA_schema.json"));

    BigQueryTableLoadService BigQueryTableLoadService =
        new BigQueryTableLoadService(FakeBigQueryServiceFactory.forStub(fakeBigquery));

    BigQueryTableLoadService.loadTable("project1.datasetA.TableA");
    BigQueryTableLoadService.loadTable("project1.datasetA.TableA");

    assertThat(fakeBigquery.numTimesCounterTablesGetExecute.intValue()).isEqualTo(1);
  }

  @Test
  public void loadTable_bigQueryEntityUnknownTable_throwsTableNotFoundException() {
    BigQueryTableLoadService bigQueryTableService =
        new BigQueryTableLoadService(
            FakeBigQueryServiceFactory.forTableSchemas(
                TestResourceLoader.load("schemas/tableA_schema.json")));

    BigQueryOperationException tableNotFoundException =
        assertThrows(BigQueryOperationException.class,
            () -> bigQueryTableService.loadTable("project1.datasetB.TableA"));

    assertThat(tableNotFoundException).hasMessageThat()
        .isEqualTo("BigQuery Operation exception for\n"
            + "BigQueryTableEntity{projectId=project1, dataset=datasetB, table=TableA}");
  }
}
