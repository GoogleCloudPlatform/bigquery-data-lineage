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

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.api.gax.rpc.TransportChannel;
import com.google.api.gax.tracing.ApiTracer;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.threeten.bp.Duration;

@AutoValue
public abstract class FakeApiCallContext implements ApiCallContext {

  @Nullable
  public abstract Credentials getCredentials();

  @Nullable
  @Override
  public final <T> T getOption(Key<T> key) { return null; }

  @Override
  public ApiCallContext withCredentials(Credentials credentials) {
    return this.toBuilder().setCredentials(credentials).build();
  }

  @Nullable
  public abstract TransportChannel getTransportChannel();

  @Override
  public ApiCallContext withTransportChannel(TransportChannel transportChannel) {
    return this.toBuilder().setTransportChannel(transportChannel).build();
  }

  @Override
  public ApiCallContext withTimeout(@Nullable Duration duration) {
    return this.toBuilder().setTimeout(duration).build();
  }

  @Override
  public ApiCallContext withStreamWaitTimeout(@Nullable Duration duration) {
    return this.toBuilder().setStreamWaitTimeout(duration).build();
  }

  @Override
  public ApiCallContext withStreamIdleTimeout(@Nullable Duration duration) {
    return this.toBuilder().setStreamIdleTimeout(duration).build();
  }

  @Override
  public ApiCallContext withTracer(@Nonnull ApiTracer apiTracer) {
    return this.toBuilder().setTracer(apiTracer).build();
  }

  @Override
  public ApiCallContext nullToSelf(ApiCallContext apiCallContext) {
    return this;
  }

  @Override
  public ApiCallContext merge(ApiCallContext apiCallContext) {
    return this;
  }

  @Override
  public ApiCallContext withExtraHeaders(Map<String, List<String>> map) {
    return this.toBuilder().setExtraHeaders(map).build();
  }

  @Override
  public ApiCallContext withRetrySettings(RetrySettings retrySettings) {
    return toBuilder().setRetrySettings(retrySettings).build();
  }

  @Override
  public ApiCallContext withRetryableCodes(Set<Code> retryableCodes) {
    return toBuilder().setRetryableCodes(retryableCodes).build();
  }

  @Override
  public final <T> ApiCallContext withOption(Key<T> key, T value) {
    return this;
  }

  public static Builder builder() {
    return new AutoValue_FakeApiCallContext.Builder();
  }

  public abstract Builder toBuilder();


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTransportChannel(@Nullable TransportChannel transportChannel);

    public abstract Builder setCredentials(@Nullable Credentials newCredentials);

    public abstract Builder setTracer(ApiTracer newTracer);

    public abstract Builder setTimeout(@Nullable Duration newTimeout);

    public abstract Builder setStreamWaitTimeout(@Nullable Duration newStreamWaitTimeout);

    public abstract Builder setStreamIdleTimeout(@Nullable Duration newStreamIdleTimeout);

    public abstract Builder setExtraHeaders(Map<String, List<String>> newExtraHeaders);

    public abstract Builder setRetrySettings(RetrySettings retrySettings);

    public abstract Builder setRetryableCodes(Set<Code> codes);

    public abstract FakeApiCallContext build();
  }
}
