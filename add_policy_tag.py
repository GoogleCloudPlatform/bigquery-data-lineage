#!/usr/bin/python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Updates a BigQuery Schema JSON with additional Policy Tag."""

import json
import sys

schema_file_name = sys.argv[1]
column_name = sys.argv[2]
additional_policy_tag = sys.argv[3]


#Read Schema JSON
schema_json_file = open(schema_file_name, 'r')
table_schema = json.load(schema_json_file)
schema_json_file.close()

#Identify Column
for column in table_schema:
  if column['name'] == column_name:
    if 'policyTags' not in column:
      column['policyTags'] = []
    column['policyTags'].append(additional_policy_tag)

# Print updated schema
print (table_schema)

# Write to the same file name
json.dump(table_schema, open(schema_file_name, 'w'), indent=4)
