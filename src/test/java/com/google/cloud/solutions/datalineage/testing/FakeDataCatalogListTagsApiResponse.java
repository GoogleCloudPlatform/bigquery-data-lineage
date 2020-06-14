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

package com.google.cloud.solutions.datalineage.testing;

import com.google.cloud.datacatalog.v1beta1.ListTagsResponse;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.common.collect.ImmutableCollection;

/**
 * Fake implementation of List Tags Response
 */
public class FakeDataCatalogListTagsApiResponse extends FakeApiFutureBase<ListTagsResponse> {

  private final ImmutableCollection<Tag> tags;

  public FakeDataCatalogListTagsApiResponse(ImmutableCollection<Tag> tags) {
    this.tags = tags;
  }

  @Override
  public ListTagsResponse get() {
    return ListTagsResponse.newBuilder().addAllTags(tags).build();
  }
}
