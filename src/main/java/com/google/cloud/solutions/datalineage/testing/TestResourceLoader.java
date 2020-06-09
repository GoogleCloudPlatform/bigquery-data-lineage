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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class TestResourceLoader {

  private static final String TEST_RESOURCE_FOLDER = "test";

  public static String load(String resourceFileName) {
    try {
      byte[] bytes = Files
          .readAllBytes(Paths.get("src", TEST_RESOURCE_FOLDER, "resources", resourceFileName));
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException ioException) {
      return "";
    }
  }
}
