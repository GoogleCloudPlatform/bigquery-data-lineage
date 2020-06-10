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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import java.io.IOException;
import java.util.Collection;

/**
 * DataCatalog based service to create or update existing Catalog Tags.
 */
public final class CatalogTagsApplicator {

  private final DataCatalogService dataCatalogService;
  private final ImmutableList<Tag> providedTags;

  /**
   * @param dataCatalogService
   * @param providedTags
   */
  private CatalogTagsApplicator(DataCatalogService dataCatalogService,
      Collection<Tag> providedTags) {
    this.dataCatalogService = checkNotNull(dataCatalogService);
    this.providedTags = ImmutableList.copyOf(providedTags);
  }

  /**
   * Creates or Updates Tags in Data Catalog fpr the provided Data {@code entryId}.
   * <p>Checks the Data Catalog for existing tags of templateId (defined in the tags) and then
   * updates the existing tags or creates new.
   *
   * @param entryId the Data Catalog Entity Id
   * @return the list of created/updated Tags.
   */
  public ImmutableList<Tag> apply(String entryId) {

    /** Helper for computing whether the tag needs to be updated or created. */
    class ApplyRunner {

      private final ImmutableTable<String, String, Tag> existingTags;

      public ApplyRunner() {
        existingTags = lookUpExistingTags();
      }

      private ImmutableTable<String, String, Tag> lookUpExistingTags() {
        ImmutableSet<String> tagsTemplateIds =
            providedTags.stream()
                .map(Tag::getTemplate)
                .collect(toImmutableSet());

        return dataCatalogService.lookUpTagsForTemplateIds(entryId, tagsTemplateIds);
      }

      Tag createOrUpdateTag(Tag tag) {
        String col = tag.getColumn();

        if (existingTags.contains(tag.getTemplate(), col)) {
          return dataCatalogService.updateTag(existingTags.get(tag.getTemplate(), col), tag);
        } else {
          return dataCatalogService.createTag(entryId, tag);
        }
      }
    }

    ApplyRunner applyRunner = new ApplyRunner();

    return providedTags.stream()
        .map(applyRunner::createOrUpdateTag)
        .collect(toImmutableList());
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for Tag Applicator, creates a new instance of DataCatalogService if stub is provided.
   */
  public static final class Builder {

    private DataCatalogService dataCatalogService;
    private ImmutableList<Tag> providedTags;
    private DataCatalogStub dataCatalogStub;


    public Builder withDataCatalogService(
        DataCatalogService dataCatalogService) {
      this.dataCatalogService = dataCatalogService;
      return this;
    }

    public Builder forTags(Collection<Tag> providedTags) {
      this.providedTags = ImmutableList.copyOf(providedTags);
      return this;
    }

    public Builder withDataCatalogStub(
        DataCatalogStub dataCatalogStub) {
      this.dataCatalogStub = dataCatalogStub;
      return this;
    }

    /**
     * Returns a new instance for given tags using the provided stub or DataCatalogService.
     *
     * @throws IOException if there is an issue with creating a DataCatalogClient from stub.
     */
    public CatalogTagsApplicator build() throws IOException {
      if (dataCatalogService == null) {
        dataCatalogService = DataCatalogService.usingStub(dataCatalogStub);
      }

      return new CatalogTagsApplicator(dataCatalogService, providedTags);
    }
  }
}
