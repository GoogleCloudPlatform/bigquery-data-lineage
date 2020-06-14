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

package com.google.cloud.solutions.datalineage.extractor;

import com.google.cloud.datacatalog.v1beta1.Tag;

/**
 * Utility methods to extract parts of Data Catalog Tag.
 */
public final class TagUtility {

  /**
   * Uses {@link Tag#getName()} field to extract parent.
   *
   * @param tag the DataCatalog Tag applied to an Entity.
   * @return the parent id.
   */
  public static String extractParent(Tag tag) {
    return tag.getName().split("/tags")[0];
  }

  private TagUtility() {
  }
}
