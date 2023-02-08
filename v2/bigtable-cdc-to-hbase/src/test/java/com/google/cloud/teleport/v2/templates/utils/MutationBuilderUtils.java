/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.utils;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

/** Builder classes to build change stream mutations and hbase mutations for tests. */
public class MutationBuilderUtils {

  /** Builder class for Hbase mutations. */
  public static class HbaseMutationBuilder {

    public static Put createPut(
        String rowKey, String colFamily, String colQualifier, String value, long atTimestamp) {
      return new Put(rowKey.getBytes(), atTimestamp)
          .addColumn(colFamily.getBytes(), colQualifier.getBytes(), value.getBytes());
    }

    public static Delete createDelete(
        String rowKey, String colFamily, String colQualifier, long atTimestamp) {
      return new Delete(rowKey.getBytes(), atTimestamp)
          .addColumns(colFamily.getBytes(), colQualifier.getBytes());
    }

    public static Delete createDeleteFamily(String rowKey, String colFamily, long atTimestamp) {
      return new Delete(rowKey.getBytes(), atTimestamp).addFamily(colFamily.getBytes());
    }
  }
}
