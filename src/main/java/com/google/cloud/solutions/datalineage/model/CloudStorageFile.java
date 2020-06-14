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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity;
import com.google.cloud.solutions.datalineage.model.LineageMessages.DataEntity.DataEntityTypes;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Value class to represent a file on Google Cloud Storage.
 */
@AutoValue
@JsonDeserialize(builder = CloudStorageFile.Builder.class)
public abstract class CloudStorageFile implements DataEntityConvertible, Serializable {

  public abstract String getBucket();

  public abstract String getFile();

  @Override
  public DataEntity dataEntity() {
    return DataEntity.newBuilder()
        .setKind(DataEntityTypes.CLOUD_STORAGE_FILE)
        .setSqlResource("gs://" + getBucket() + "/" + getFile())
        .setLinkedResource("storage.googleapis.com/" + getBucket() + "/" + getFile())
        .build();
  }

  public static CloudStorageFile create(String fileName) {

    Matcher matcher = Pattern.compile("^gs://(?<bucket>[^/]+)/(?<file>.*)$").matcher(fileName);

    if (!matcher.find()) {
      throw new IllegalArgumentException(
          String
              .format("fileName (%s) needs to start with gs://<bucketName>/<filepath>", fileName));
    }

    return builder()
        .setBucket(matcher.group("bucket"))
        .setFile(matcher.group("file")).build();
  }

  @JsonCreator
  public static Builder builder() {
    return new AutoValue_CloudStorageFile.Builder();
  }

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "set")
  public abstract static class Builder {

    public abstract Builder setBucket(String bucket);

    public abstract Builder setFile(String file);

    public abstract CloudStorageFile build();
  }


}
