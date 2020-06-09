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

package com.google.cloud.solutions.datalineage.converter;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.solutions.datalineage.model.LineageMessages.CompositeLineage;
import java.io.Serializable;

/**
 * Factory to build TagConverters for DataCatalog Tags.
 */
public final class LineageToTagConverterFactory implements Serializable {

  private final String tagTemplateId;

  private LineageToTagConverterFactory(String tagTemplateId) {
    checkArgument(isNotBlank(tagTemplateId), "Provide non-blank tagTemplateName");
    this.tagTemplateId = tagTemplateId;
  }

  public static LineageToTagConverterFactory forTemplateId(String lineageTagTemplateId) {
    return new LineageToTagConverterFactory(lineageTagTemplateId);
  }


  public CompositeLineageToTagConverter converterFor(CompositeLineage compositeLineage) {
    return new CompositeLineageToTagConverter(tagTemplateId, compositeLineage);
  }
}
