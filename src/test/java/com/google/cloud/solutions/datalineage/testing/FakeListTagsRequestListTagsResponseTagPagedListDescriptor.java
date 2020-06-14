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

import com.google.api.gax.rpc.PagedListDescriptor;
import com.google.cloud.datacatalog.v1beta1.ListTagsRequest;
import com.google.cloud.datacatalog.v1beta1.ListTagsResponse;
import com.google.cloud.datacatalog.v1beta1.Tag;

class FakeListTagsRequestListTagsResponseTagPagedListDescriptor implements
    PagedListDescriptor<ListTagsRequest, ListTagsResponse, Tag> {

  @Override
  public String emptyToken() {
    return ListTagsRequest.getDefaultInstance().getPageToken();
  }

  @Override
  public ListTagsRequest injectToken(ListTagsRequest listTagsRequest, String s) {
    return listTagsRequest.toBuilder().setPageToken(s).build();
  }

  @Override
  public ListTagsRequest injectPageSize(ListTagsRequest listTagsRequest, int i) {
    return listTagsRequest.toBuilder().setPageSize(i).build();
  }

  @Override
  public Integer extractPageSize(ListTagsRequest listTagsRequest) {
    return listTagsRequest.getPageSize();
  }

  @Override
  public String extractNextToken(ListTagsResponse listTagsResponse) {
    return listTagsResponse.getNextPageToken();
  }

  @Override
  public Iterable<Tag> extractResources(ListTagsResponse listTagsResponse) {
    return listTagsResponse.getTagsList();
  }
}
