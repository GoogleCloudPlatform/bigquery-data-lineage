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

package com.google.cloud.solutions.datalineage.testing;

import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseAsList;
import static com.google.cloud.solutions.datalineage.converter.ProtoJsonConverter.parseJson;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient.ListTagsPagedResponse;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.ListTagsRequest;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.datalineage.extractor.TagUtility;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class FakeDataCatalogStub extends DataCatalogStub implements Serializable {

  private boolean shutdown;
  private boolean terminated;

  private final ImmutableMap<String, Entry> predefinedEntries;
  private final ImmutableMultimap<String, Tag> predefinedTags;

  public FakeDataCatalogStub(
      ImmutableCollection<Entry> predefinedEntries,
      ImmutableMultimap<String, Tag> predefinedTags) {
    this.predefinedEntries = predefinedEntries.stream()
        .collect(toImmutableMap(Entry::getLinkedResource, identity()));

    this.predefinedTags = predefinedTags;
  }

  public static FakeDataCatalogStub buildWithTestData(
      List<String> entryResourcesNames,
      List<String> tagsResourceNames) {

    ImmutableList<Entry> entries =
        entryResourcesNames.stream()
            .map(TestResourceLoader::load)
            .map(json -> parseJson(json, Entry.class))
            .collect(toImmutableList());

    ImmutableListMultimap<String, Tag> entityTags =
        tagsResourceNames.stream()
            .map(TestResourceLoader::load)
            .map(tagsJson -> parseAsList(tagsJson, Tag.class))
            .flatMap(List::stream)
            .collect(toImmutableListMultimap(TagUtility::extractParent, identity()));

    return new FakeDataCatalogStub(entries, entityTags);
  }


  @Override
  public UnaryCallable<LookupEntryRequest, Entry> lookupEntryCallable() {
    checkState(!shutdown, "Stub shutdown");

    return new UnaryCallable<LookupEntryRequest, Entry>() {

      @Override
      public ApiFuture<Entry> futureCall(LookupEntryRequest lookupEntryRequest,
          ApiCallContext apiCallContext) {
        return new FakeDataCatalogLookupEntryResponse(
            predefinedEntries.get(lookupEntryRequest.getLinkedResource()),
            lookupEntryRequest);
      }
    };
  }

  @Override
  public UnaryCallable<ListTagsRequest, ListTagsPagedResponse> listTagsPagedCallable() {
    checkState(!shutdown, "Stub shutdown");

    return new UnaryCallable<ListTagsRequest, ListTagsPagedResponse>() {
      @Override
      public ApiFuture<ListTagsPagedResponse> futureCall(ListTagsRequest listTagsRequest,
          ApiCallContext apiCallContext) {
        return
            new FakeDataCatalogPagesListTagsResponse(
                listTagsRequest, apiCallContext, predefinedTags.get(listTagsRequest.getParent()));
      }
    };
  }

  @Override
  public void close() {
  }

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return terminated;
  }

  @Override
  public void shutdownNow() {
    shutdown();
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) {
    terminated = true;
    return true;
  }


}
