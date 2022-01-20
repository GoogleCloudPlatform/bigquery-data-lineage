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

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.PageContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.gax.tracing.ApiTracer;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient.ListTagsPagedResponse;
import com.google.cloud.datacatalog.v1beta1.ListTagsRequest;
import com.google.cloud.datacatalog.v1beta1.ListTagsResponse;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.ExecutionException;
import org.threeten.bp.Duration;

public class FakeDataCatalogPagesListTagsResponse extends FakeApiFutureBase<ListTagsPagedResponse> {

  private final ListTagsRequest request;
  private final ImmutableCollection<Tag> tags;
  private final ApiCallContext callContext;

  public FakeDataCatalogPagesListTagsResponse(
      ListTagsRequest request,
      ApiCallContext callContext,
      ImmutableCollection<Tag> tags) {
    this.request = request;
    this.tags = tags;
    this.callContext = buildCallContext(callContext);
  }

  private static ApiCallContext buildCallContext(ApiCallContext context) {

    if (context == null) {
      return FakeApiCallContext.builder().setTracer(new ApiTracer() {
        @Override
        public Scope inScope() {
          return null;
        }

        @Override
        public void operationSucceeded() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void operationCancelled() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void operationFailed(Throwable throwable) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void connectionSelected(String s) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptStarted(int i) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptStarted(Object o, int i) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptSucceeded() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptCancelled() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptFailed(Throwable throwable, Duration duration) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptFailedRetriesExhausted(Throwable throwable) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void attemptPermanentFailure(Throwable throwable) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void lroStartFailed(Throwable throwable) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void lroStartSucceeded() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void responseReceived() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void requestSent() {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }

        @Override
        public void batchRequestSent(long l, long l1) {
          // Do nothing because this is a Fake and doesn't implement an actual gRPC operation.
        }
      }).setExtraHeaders(ImmutableMap.of()).build();
    }

    return context;
  }

  @Override
  public ListTagsPagedResponse get() throws ExecutionException, InterruptedException {
    return ListTagsPagedResponse
        .createAsync(
            buildPageContext(),
            new FakeDataCatalogListTagsApiResponse(tags))
        .get();
  }

  private PageContext<ListTagsRequest, ListTagsResponse, Tag> buildPageContext() {
    return PageContext.create(
        new UnaryCallable<ListTagsRequest, ListTagsResponse>() {
          @Override
          public ApiFuture<ListTagsResponse> futureCall(ListTagsRequest listTagsRequest,
              ApiCallContext apiCallContext) {
            return new FakeDataCatalogListTagsApiResponse(tags);
          }
        },
        new FakeListTagsRequestListTagsResponseTagPagedListDescriptor(),
        request,
        callContext);
  }

}
