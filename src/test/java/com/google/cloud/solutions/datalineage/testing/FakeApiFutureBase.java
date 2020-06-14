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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

public abstract class FakeApiFutureBase<V> implements ApiFuture<V> {

  @Override
  public final void addListener(Runnable runnable, Executor executor) {
    executor.execute(runnable);
  }

  @Override
  public final boolean cancel(boolean b) {
    return false;
  }

  @Override
  public final boolean isCancelled() {
    return false;
  }

  @Override
  public final boolean isDone() {
    return true;
  }

  @Override
  public final V get(long l, @Nonnull TimeUnit timeUnit)
      throws InterruptedException, ExecutionException {
    return get();
  }
}
