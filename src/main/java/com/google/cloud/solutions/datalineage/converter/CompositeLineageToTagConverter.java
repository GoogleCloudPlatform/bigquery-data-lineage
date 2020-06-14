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

package com.google.cloud.solutions.datalineage.converter;

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.asJsonString;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.TagField;
import com.google.cloud.solutions.datalineage.model.LineageMessages.ColumnLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import com.google.cloud.solutions.datalineage.model.LineageMessages.JobInformation;
import com.google.cloud.solutions.datalineage.model.LineageMessages.TableLineage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Adapter class to convert a Composite Lineage information into Data Catalog Tags for Entity and
 * Columns.
 */
public final class CompositeLineageToTagConverter {

  private final String tagTemplateId;
  private final CompositeLineage compositeLineage;
  private final JobInformation jobInformation;
  private final TableLineage tableLineage;
  private final ImmutableList<ColumnLineage> columnsLineage;

  /**
   * Build an instance of the tag converter.
   *
   * @param tagTemplateId    the Data Catalog tag template id, in form of {@code
   *                         /projects/<projectId>/locations/<location>/...}
   * @param compositeLineage the composite lineage which needs to be converted.
   */
  public CompositeLineageToTagConverter(String tagTemplateId, CompositeLineage compositeLineage) {
    this.tagTemplateId = tagTemplateId;
    this.compositeLineage = compositeLineage;
    this.jobInformation = compositeLineage.getJobInformation();
    this.tableLineage = compositeLineage.getTableLineage();
    this.columnsLineage = ImmutableList.copyOf(compositeLineage.getColumnsLineageList());
  }

  /**
   * Returns complete set of Data Catalog Tags including both TableLineageTags and ColumnLineage
   * Tags.
   */
  public ImmutableSet<Tag> buildTags() {
    ImmutableSet.Builder<Tag> tagsBuilder = ImmutableSet.builder();

    tagsBuilder.add(buildTableTag());

    if (columnsLineage != null) {
      columnsLineage.stream()
          .map(this::buildColumnTag)
          .forEach(tagsBuilder::add);
    }

    return tagsBuilder.build();
  }

  private Tag buildTableTag() {
    Tag.Builder tagBuilder = Tag.newBuilder();
    addReconcileTime(tagBuilder);
    addJobInformation(tagBuilder);
    addParents(tagBuilder, tableLineage.getParentsList());
    return tagBuilder.build();
  }

  private Tag buildColumnTag(ColumnLineage columnLineage) {
    Tag.Builder tagBuilder = Tag.newBuilder();
    addReconcileTime(tagBuilder);
    addJobInformation(tagBuilder);
    addParents(tagBuilder, columnLineage.getParentsList());
    tagBuilder.setColumn(columnLineage.getTarget().getColumn());
    return tagBuilder.build();
  }

  private static void addParents(Tag.Builder tagBuilder, List<?> parents) {
    if (parents == null || parents.isEmpty()) {
      return;
    }

    tagBuilder.putFields(
        "parents",
        TagField.newBuilder()
            .setStringValue(asJsonString(parents))
            .build());
  }

  private void addReconcileTime(Tag.Builder tagBuilder) {
    tagBuilder
        .putFields("reconcileTime",
            TagField.newBuilder()
                .setTimestampValue(convertToTimestamp((compositeLineage.getReconcileTime())))
                .build());
  }

  private void addJobInformation(Tag.Builder tagBuilder) {
    tagBuilder
        .setTemplate(tagTemplateId)
        .putFields("jobTime",
            TagField.newBuilder()
                .setTimestampValue(convertToTimestamp(jobInformation.getJobTime()))
                .build())
        .putFields("actuator",
            TagField.newBuilder().setStringValue(jobInformation.getActuator()).build());

    if (isNotBlank(jobInformation.getJobId())) {
      tagBuilder
          .putFields("jobId",
              TagField.newBuilder().setStringValue(jobInformation.getJobId()).build());
    }
  }

  private static Timestamp convertToTimestamp(Long instant) {
    if (instant == null) {
      return Timestamp.newBuilder().setSeconds(0).build();
    }

    return Timestamp.newBuilder()
        .setSeconds(Instant.ofEpochMilli(instant).getEpochSecond())
        .build();
  }
}