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

import com.google.gson.JsonParser;

/**
 * Provides assert for JSON comparison using Google's Gson library.
 */
public final class JsonAssert {

  /**
   * Compares the two strings as JSON by converting them into Gson. Throws an AssertionError if they
   * are not equal.
   *
   * @param actual   the actual of the Method Under Test.
   * @param expected the expected output JSON.
   */
  public static void assertJsonEquals(String actual, String expected) {
    if (expected == null || actual == null) {
      throw new NullPointerException(
          String.format("expected (%s) or actual (%s) is null", expected, actual));
    }

    if (!JsonParser.parseString(actual).equals(JsonParser.parseString(expected))) {
      String assertionMessage = String
          .format("JSON mismatch\nactual:\n%s\ndoes not match expected:\n%s", actual, expected);
      throw new AssertionError(assertionMessage);
    }
  }

}
