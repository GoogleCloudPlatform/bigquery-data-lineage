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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class FakeDataCatalogLookupEntryResponse extends FakeApiFutureBase<Entry> {

  private final Entry entry;
  private final LookupEntryRequest request;

  public FakeDataCatalogLookupEntryResponse(Entry entry, LookupEntryRequest request) {
    this.entry = entry;
    this.request = request;
  }

  @Override
  public Entry get() throws ExecutionException {

    if (isBlank(request.getLinkedResource())) {
      throw new UnsupportedOperationException(
          "Sql Resource based search is not supported in fake");
    }

    if (entry == null) {
      String msg = String.format("Entry Not Found: (%s)", request.getLinkedResource());
      throw new ExecutionException(msg, new IOException(msg));
    }

    return entry;
  }

}
