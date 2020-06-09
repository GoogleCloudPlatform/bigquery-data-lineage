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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient.ListTagsPage;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.flogger.FluentLogger;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * Utility class to encapsulate DataCatalog operations.
 */
public class DataCatalogService {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final DataCatalogClient dataCatalogClient;

  private DataCatalogService(DataCatalogClient dataCatalogClient) {
    this.dataCatalogClient = dataCatalogClient;
  }

  public static DataCatalogService using(DataCatalogClient catalogClient) {
    return new DataCatalogService(catalogClient);
  }

  public static DataCatalogService usingStub(DataCatalogStub catalogStub) throws IOException {
    return using(
        (catalogStub == null) ?
            DataCatalogClient.create() :
            DataCatalogClient.create(catalogStub));
  }

  public void applyEntityTags(String entryId, Collection<Tag> tags) {
    ImmutableSet<String> lookupTagTemplateIds =
        tags.stream()
            .map(Tag::getTemplate)
            .collect(toImmutableSet());

    ImmutableTable<String, String, Tag> existingTags = lookUpTags(entryId, lookupTagTemplateIds);

    tags.forEach
        (tag -> {
              String col = tag.getColumn();

              if (existingTags.contains(tag.getTemplate(), col)) {
                updateTag(existingTags.get(tag.getTemplate(), col), tag);
              } else {
                createTag(entryId, tag);
              }
            }
        );
  }


  public Tag updateTag(Tag existingTag, Tag newTag) {
    logger.atInfo().atMostEvery(30, TimeUnit.SECONDS)
        .log("updating tag %s:\n%s", existingTag, newTag);
    return dataCatalogClient
        .updateTag(
            Tag.newBuilder(existingTag).mergeFrom(newTag).build(),
            FieldMask.getDefaultInstance());
  }

  public Tag createTag(String entryId, Tag newTag) {
    logger.atInfo().atMostEvery(30, TimeUnit.SECONDS)
        .log("creating tag for %s\n%s", entryId, newTag);
    return dataCatalogClient.createTag(entryId, newTag);
  }

  public Optional<Entry> lookupEntry(DataEntity entity) {
    try {
      return Optional.ofNullable(dataCatalogClient.lookupEntry(
          LookupEntryRequest.newBuilder()
              .setLinkedResource(entity.getLinkedResource())
              .build()));
    } catch (Exception exception) {
      logger.atSevere().atMostEvery(1, TimeUnit.MINUTES)
          .withCause(exception)
          .log("Error retrieving entry for \n%s", entity);
      return Optional.empty();
    }
  }

  public ImmutableTable<String, String, Tag> lookUpTags(
      DataEntity entity,
      ImmutableCollection<String> lookUpTemplateIds) {
    return lookupEntry(entity)
        .map(entry -> lookUpTags(entry, lookUpTemplateIds))
        .orElseGet(ImmutableTable::of);
  }

  public ImmutableTable<String, String, Tag> lookUpTags(
      Entry tableEntry,
      ImmutableCollection<String> lookUpTemplateIds) {
    return lookUpTags(tableEntry.getName(), lookUpTemplateIds);
  }

  public ImmutableTable<String, String, Tag> lookUpTags(
      String tableEntryId,
      ImmutableCollection<String> lookUpTemplateIds) {
    ImmutableTable.Builder<String, String, Tag> tagMapBuilder = ImmutableTable.builder();

    Iterator<ListTagsPage> pageIterator =
        dataCatalogClient.listTags(tableEntryId)
            .iteratePages()
            .iterator();

    //noinspection WhileLoopReplaceableByForEach
    while (pageIterator.hasNext()) {
      StreamSupport
          .stream(pageIterator.next().getValues().spliterator(),/*parallel=*/ true)
          .filter(tag -> lookUpTemplateIds.contains(tag.getTemplate()))
          .forEach(tag -> tagMapBuilder.put(tag.getTemplate(), tag.getColumn(), tag));
    }

    return tagMapBuilder.build();
  }

  /**
   * Validates the provided Tag Template Id against a given pattern.
   *
   * @param tagTemplateId the Data Catalog Tag Template Id.
   * @return the same template Id if valid.
   * @throws IllegalArgumentException if the template is not as per valid Pattern.
   */
  public static String validateTemplateId(String tagTemplateId) {
    Matcher matcher =
        Pattern.compile(
            "^projects/(?<projectId>[a-zA-Z0-9-]+)/locations/(?<location>[a-z0-9-]+)/tagTemplates/(?<templateId>[a-zA-Z_]+)$")
            .matcher(tagTemplateId);

    if (!matcher.find()) {
      throw new IllegalArgumentException(
          String.format("Given Template Id (%S) not in format\n(%s)",
              tagTemplateId,
              matcher.pattern()));
    }

    return tagTemplateId;
  }

  public static void validateTemplateIds(Collection<String> tagTemplateIds) {
    tagTemplateIds.forEach(DataCatalogService::validateTemplateId);
  }

}
