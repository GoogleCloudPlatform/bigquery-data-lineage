#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM maven:3-openjdk-11 AS build_and_test
COPY . /bigquery-data-lineage-src
WORKDIR /bigquery-data-lineage-src
RUN mvn clean generate-sources compile package

FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest
ARG MAIN_CLASS
COPY --from=build_and_test /bigquery-data-lineage-src/target/bigquery-data-lineage-bundled-0.1-SNAPSHOT.jar .

ENV FLEX_TEMPLATE_JAVA_CLASSPATH="bigquery-data-lineage-bundled-0.1-SNAPSHOT.jar"
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="${MAIN_CLASS}"