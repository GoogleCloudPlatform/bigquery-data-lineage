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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class TagsForCatalog {

  public abstract String getEntryId();

  public abstract ImmutableList<String> getTagsJson();

  public static TagsForCatalog empty() {
    return create("", ImmutableList.of());
  }

  public final boolean isEmpty() {
    return this.equals(empty());
  }

  public final ImmutableList<Tag> parsedTags() {
    return ProtoJsonConverter.parseAsList(getTagsJson(), Tag.class);
  }

  @SchemaCreate
  public static TagsForCatalog create(String entryId, Collection<String> tagsJson) {
    return builder()
        .setEntryId(entryId)
        .setTagsJson(ImmutableList.copyOf(tagsJson))
        .build();
  }

  public static Builder builder() {
    return new AutoValue_TagsForCatalog.Builder();
  }

  public static Builder forTags(Collection<Tag> tags) {
    return builder()
        .setTagsJson(
            tags.stream()
                .map(ProtoJsonConverter::asJsonString)
                .collect(toImmutableList()));
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEntryId(String newEntryId);

    public Builder setEntry(Entry entry) {
      return setEntryId(entry.getName());
    }

    public abstract Builder setTagsJson(ImmutableList<String> newTagsJson);

    public abstract TagsForCatalog build();
  }
}
