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

package com.google.cloud.solutions.datalineage.service;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableTable.toImmutableTable;
import static java.util.function.Function.identity;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.datalineage.exception.BigQueryOperationException;
import com.google.cloud.solutions.datalineage.extractor.BigQueryTableCreator;
import com.google.cloud.solutions.datalineage.model.BigQueryTableEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnPolicyTags;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TargetPolicyTags;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table.Cell;
import com.google.common.flogger.GoogleLogger;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper on top of Bigquery API to provide <b>Policy Tag</b> related operations.
 */
public final class BigQueryPolicyTagService implements Serializable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String POLICY_TAG_FIELD_NAME = "policyTags";
  private static final String POLICY_IDS_FIELD_NAME = "names";
  private static final String POLICY_TAG_IDS_JSON_PATH = "$." + POLICY_IDS_FIELD_NAME + "[*]";


  private final BigQueryServiceFactory bqServiceFactory;
  private final BigQueryTableLoadService tableLoadService;

  public BigQueryPolicyTagService(BigQueryServiceFactory bqServiceFactory) {
    this.bqServiceFactory = bqServiceFactory;
    this.tableLoadService = BigQueryTableLoadService.usingServiceFactory(bqServiceFactory);
  }

  public static BigQueryPolicyTagService usingServiceFactory(
      BigQueryServiceFactory serviceFactory) {
    return new BigQueryPolicyTagService(serviceFactory);
  }

  public ImmutableList<ColumnPolicyTags> readPolicies(BigQueryTableEntity tableEntity) {
    return readPolicies(tableEntity, ImmutableSet.of());
  }

  private ImmutableList<ColumnPolicyTags> readPolicies
      (BigQueryTableEntity tableEntity,
          Collection<String> monitoredTags) {
    PolicyExtractor policyExtractor = new PolicyExtractor(monitoredTags);

    return tableLoadService.loadTable(tableEntity)
        .getSchema()
        .getFields()
        .stream()
        .filter(policyExtractor::columnFilter)
        .map(policyExtractor::extractColumnPolicy)
        .collect(toImmutableList());
  }

  public PolicyUpdateFinder finderForLineage(CompositeLineage lineage) {

    return new PolicyUpdateFinder(lineage);
  }

  public class PolicyUpdateFinder {

    private final CompositeLineage lineage;

    public PolicyUpdateFinder(
        CompositeLineage lineage) {
      this.lineage = lineage;
    }

    private ImmutableTable<DataEntity, String, ColumnPolicyTags> readPolicyMap(
        Collection<DataEntity> tableEntities, Collection<String> monitoredPolicyTags) {
      return
          tableEntities.stream()
              .map(DataEntity::getLinkedResource)
              .map(BigQueryTableCreator::usingBestEffort)
              .map(bqTable -> readPolicyMap(bqTable, monitoredPolicyTags))
              .flatMap(t -> t.cellSet().stream())
              .collect(
                  toImmutableTable(
                      Cell::getRowKey,
                      Cell::getColumnKey,
                      Cell::getValue));
    }

    private ImmutableTable<DataEntity, String, ColumnPolicyTags> readPolicyMap(
        BigQueryTableEntity tableEntity,
        Collection<String> monitoredPolicyIds) {

      return readPolicies(tableEntity, monitoredPolicyIds).stream()
          .collect(
              toImmutableTable(policyTags -> tableEntity.dataEntity(), ColumnPolicyTags::getColumn,
                  identity()));
    }

    public Optional<TargetPolicyTags> forPolicies(Collection<String> monitoredPolicyIds) {

      if (monitoredPolicyIds == null || monitoredPolicyIds.isEmpty()) {
        logger.atInfo().atMostEvery(1, TimeUnit.MINUTES).log("no-op as empty monitored tags.");
        return Optional.empty();
      }

      ImmutableTable<DataEntity, String, ColumnPolicyTags> sourcePolicyTags =
          readPolicyMap(lineage.getTableLineage().getParentsList(),
              monitoredPolicyIds);

      HashMap<String, HashSet<String>> tagsForTargetTable = new HashMap<>();

      for (ColumnLineage columnLineage : lineage.getColumnsLineageList()) {
        for (ColumnEntity parent : columnLineage.getParentsList()) {

          ColumnPolicyTags policyTags =
              sourcePolicyTags.get(parent.getTable(), parent.getColumn());

          HashSet<String> targetPolicies =
              MoreObjects.firstNonNull(
                  tagsForTargetTable.get(columnLineage.getTarget().getColumn()),
                  new HashSet<>());

          if (policyTags != null) {
            targetPolicies.addAll(policyTags.getPolicyTagIdsList());
          }
          tagsForTargetTable
              .put(columnLineage.getTarget().getColumn(), targetPolicies);
        }
      }

      ImmutableList<ColumnPolicyTags> targetPolicyTags =
          tagsForTargetTable.entrySet().stream()
              .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
              .map(entry ->
                  ColumnPolicyTags.newBuilder()
                      .setColumn(entry.getKey())
                      .addAllPolicyTagIds(entry.getValue())
                      .build())
              .collect(toImmutableList());

      if (tagsForTargetTable.isEmpty()) {
        logger.atInfo().every(100)
            .log("Empty Target tags\nMonitoredTags:%s\ntarget:%s\nparents:\n%s",
                monitoredPolicyIds,
                lineage.getTableLineage().getTarget(),
                lineage.getColumnsLineageList());
      }

      return targetPolicyTags.isEmpty() ?
          Optional.empty() :
          Optional.of(
              TargetPolicyTags.newBuilder()
                  .setTable(lineage.getTableLineage().getTarget())
                  .addAllPolicyTags(targetPolicyTags)
                  .build());
    }
  }

  private static class PolicyExtractor {

    final ImmutableSet<String> monitoredPolicies;

    public PolicyExtractor(Collection<String> monitoredPolicies) {
      this.monitoredPolicies = ImmutableSet.copyOf(monitoredPolicies);
    }

    private boolean columnFilter(TableFieldSchema field) {
      return field.get(POLICY_TAG_FIELD_NAME) != null;
    }

    private boolean policyFilter(String policyId) {
      return monitoredPolicies.isEmpty() || monitoredPolicies.contains(policyId);
    }

    public ColumnPolicyTags extractColumnPolicy(TableFieldSchema field) {

      ImmutableList<String> policyTagIds =
          JsonPath.parse(field.get(POLICY_TAG_FIELD_NAME))
              .<List<String>>read(POLICY_TAG_IDS_JSON_PATH)
              .stream()
              .filter(this::policyFilter)
              .collect(toImmutableList());

      return ColumnPolicyTags.newBuilder()
          .setColumn(field.getName())
          .addAllPolicyTagIds(policyTagIds)
          .build();
    }
  }

  public SchemaUpdateProcessor updatePoliciesForTable(BigQueryTableEntity tableEntity) {
    return new SchemaUpdateProcessor(tableEntity);
  }

  public class SchemaUpdateProcessor {

    final BigQueryTableEntity tableEntity;
    final Table table;

    public SchemaUpdateProcessor(BigQueryTableEntity tableEntity) {
      this.tableEntity = tableEntity;
      this.table = tableLoadService.loadTable(tableEntity);
    }


    public UpdateColumnsPolicyProcessor withPolicies(List<ColumnPolicyTags> updatedPolicies) {
      ImmutableSet.Builder<String> updatableColumnsBuilder = ImmutableSet.builder();
      ImmutableMap.Builder<String, List<String>> updatedTagsForColumnsBuilder = ImmutableMap
          .builder();

      for (ColumnPolicyTags columnPolicyTags : updatedPolicies) {
        updatableColumnsBuilder.add(columnPolicyTags.getColumn());
        updatedTagsForColumnsBuilder
            .put(columnPolicyTags.getColumn(), columnPolicyTags.getPolicyTagIdsList());
      }

      return new UpdateColumnsPolicyProcessor(updatableColumnsBuilder.build(),
          updatedTagsForColumnsBuilder.build());
    }

    public class UpdateColumnsPolicyProcessor {

      final ImmutableSet<String> updatableColumns;
      final ImmutableMap<String, List<String>> updatedColumnPolicyMap;

      public UpdateColumnsPolicyProcessor(
          ImmutableSet<String> updatableColumns,
          ImmutableMap<String, List<String>> updatedColumnPolicyMap) {
        this.updatableColumns = updatableColumns;
        this.updatedColumnPolicyMap = updatedColumnPolicyMap;
      }

      public Table apply() {
        try {
          return writeToBigquery(
              table.getSchema().getFields().stream()
                  .map(this::checkAndApplyPolicy)
                  .collect(toImmutableList()));
        } catch (IOException ioException) {
          throw new BigQueryOperationException(tableEntity, ioException);
        }
      }

      private Table writeToBigquery(List<TableFieldSchema> updatedFields) throws IOException {
        return bqServiceFactory.buildService()
            .tables()
            .patch(
                table.getTableReference().getProjectId(),
                table.getTableReference().getDatasetId(),
                table.getTableReference().getTableId(),
                new Table().setSchema(new TableSchema().setFields(updatedFields)))
            .execute();
      }

      private TableFieldSchema checkAndApplyPolicy(TableFieldSchema field) {
        if (updatableColumns.contains(field.getName())) {
          return updateColumnWithPolicy(field);
        }

        return field;
      }

      @SuppressWarnings("unchecked")
      private TableFieldSchema updateColumnWithPolicy(TableFieldSchema field) {
        HashMap<String, Object> policyTags = new HashMap<>();
        HashSet<String> updatedPolicies = new HashSet<>(
            updatedColumnPolicyMap.get(field.getName()));

        Optional.ofNullable(field.get(POLICY_TAG_FIELD_NAME))
            .ifPresent(existingPolicyTags -> policyTags.putAll((Map) existingPolicyTags));

        Optional.ofNullable(policyTags.get(POLICY_IDS_FIELD_NAME))
            .ifPresent(
                existingPolicies -> updatedPolicies.addAll((Collection<String>) existingPolicies));

        policyTags.put(POLICY_IDS_FIELD_NAME, updatedPolicies);
        field.set(POLICY_TAG_FIELD_NAME, policyTags);
        return field;
      }

    }
  }
}
