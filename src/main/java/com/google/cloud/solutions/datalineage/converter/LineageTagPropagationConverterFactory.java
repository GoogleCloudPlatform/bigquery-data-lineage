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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.TagsForCatalog;
import com.google.cloud.solutions.datalineage.service.DataCatalogService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import java.util.Optional;

/**
 * Factory to create Lineage Processors for identifying Tags applied to Source Tables in Data
 * Catalog.
 */
@AutoValue
public abstract class LineageTagPropagationConverterFactory {

  abstract CompositeLineage lineage();

  abstract ImmutableList<String> monitoredSourceTags();

  abstract DataCatalogService dataCatalogService();


  public final Processor processor() {
    return new Processor(lookUpAllSourceTags());
  }

  public final class Processor {

    private final ImmutableMap<DataEntity, Table<String, String, Tag>> allMonitoredSourceTags;
    private HashBasedTable<String, String, Tag> propagationTags;

    public Processor(
        ImmutableMap<DataEntity, Table<String, String, Tag>> allMonitoredSourceTags) {
      this.allMonitoredSourceTags = allMonitoredSourceTags;
    }

    public synchronized TagsForCatalog propagationTags() {
      propagationTags = HashBasedTable.create();

      Optional<Entry> target =
          dataCatalogService()
              .lookupEntry(lineage().getTableLineage().getTarget());

      if (!target.isPresent()) {
        return TagsForCatalog.empty();
      }

      processTableLevel();
      processColumnLevel();

      return TagsForCatalog
          .forTags(ImmutableSet.copyOf(propagationTags.values()))
          .setEntry(target.get())
          .build();
    }

    private void processTableLevel() {
      lineage().getTableLineage().getParentsList()
          .stream()
          .map(allMonitoredSourceTags::get)
          .flatMap(table -> table.column("").values().stream())
          .forEach(tag -> processTag("", tag));
    }

    private void processColumnLevel() {
      for (ColumnLineage colLineage : lineage().getColumnsLineageList()) {
        String targetColumn = colLineage.getTarget().getColumn();
        for (ColumnEntity sourceColumn : colLineage.getParentsList()) {
          allMonitoredSourceTags
              .get(sourceColumn.getTable())
              .column(sourceColumn.getColumn())
              .values()
              .forEach(tag -> processTag(targetColumn, tag));
        }
      }
    }

    private void processTag(String targetColumn, Tag tag) {
      Tag.Builder updatedTagBuilder =
          firstNonNull(
              propagationTags.get(tag.getTemplate(), targetColumn),
              Tag.getDefaultInstance())
              .toBuilder();

      updatedTagBuilder
          .mergeFrom(tag)
          .clearName()
          .clearTemplateDisplayName()
          .setColumn(targetColumn);

      propagationTags.put(tag.getTemplate(), targetColumn, updatedTagBuilder.build());
    }

  }

  private ImmutableMap<DataEntity, Table<String, String, Tag>> lookUpAllSourceTags() {
    return
        lineage().getTableLineage()
            .getParentsList()
            .stream()
            .collect(toImmutableMap(identity(), this::lookUpMonitoredTags));
  }

  private Table<String, String, Tag> lookUpMonitoredTags(DataEntity dataEntity) {
    return dataCatalogService().lookUpTags(dataEntity, monitoredSourceTags());
  }

  public static Builder builder() {
    return new AutoValue_LineageTagPropagationConverterFactory.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder lineage(CompositeLineage lineage);

    public abstract Builder monitoredSourceTags(ImmutableList<String> monitoredSourceTags);

    public abstract Builder dataCatalogService(DataCatalogService dataCatalogService);

    public abstract LineageTagPropagationConverterFactory build();
  }
}
