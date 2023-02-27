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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.timeT;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value2;

import com.google.cloud.teleport.v2.templates.transforms.HbaseRowMutationIO;
import com.google.cloud.teleport.v2.templates.utils.HbaseUtils;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsCoder;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for Hbase row mutation IO. */
@RunWith(JUnit4.class)
public class HbaseRowMutationIOTest {

  private static final Logger log = LoggerFactory.getLogger(HbaseRowMutationIOTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static HBaseTestingUtility hbaseTestingUtil;

  public HbaseRowMutationIOTest() {}

  @BeforeClass
  public static void setUpCluster() throws Exception {
    // Create an HBase test cluster with one table.
    hbaseTestingUtil = new HBaseTestingUtility();
    hbaseTestingUtil.startMiniCluster();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    // Provide custom encoder to non-serializable RowMutations class.
    pipeline.getCoderRegistry().registerCoderForClass(RowMutations.class, RowMutationsCoder.of());
  }

  @Test
  public void writesPuts() throws Exception {

    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    // Write two cells in one row mutations object
    RowMutations rowMutationsOnTwoColumnFamilies = new RowMutations(rowKey.getBytes());
    rowMutationsOnTwoColumnFamilies.add(
        Arrays.asList(
            HbaseUtils.HbaseMutationBuilder.createPut(
                rowKey, colFamily, colQualifier, value, timeT),
            HbaseUtils.HbaseMutationBuilder.createPut(
                rowKey, colFamily2, colQualifier2, value2, timeT)));

    // Two mutations on same cell, later one should overwrite earlier one
    RowMutations overwritingRowMutations =
        new RowMutations(rowKey2.getBytes())
            .add(
                Arrays.asList(
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier, value, timeT),
                    // Overwrites previous mutation
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier, value2, timeT)));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey.getBytes(), rowMutationsOnTwoColumnFamilies),
                KV.of(rowKey2.getBytes(), overwritingRowMutations)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertEquals(2, HbaseUtils.getRowResult(table, rowKey).size());
    Assert.assertEquals(value, HbaseUtils.getCell(table, rowKey, colFamily, colQualifier));
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily2, colQualifier2));

    Assert.assertEquals(1, HbaseUtils.getRowResult(table, rowKey2).size());
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey2, colFamily, colQualifier));
  }

  @Test
  public void writesDeletes() throws Exception {
    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    // Expect deletes to result in empty row.
    RowMutations deleteCellMutation =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey, colFamily, colQualifier, value, timeT),
                    HbaseUtils.HbaseMutationBuilder.createDelete(
                        rowKey, colFamily, colQualifier, timeT)));
    // Expect delete family to delete entire row.
    RowMutations deleteColFamilyMutation =
        new RowMutations(rowKey2.getBytes())
            .add(
                Arrays.asList(
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier, value, timeT),
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier2, value2, timeT),
                    HbaseUtils.HbaseMutationBuilder.createDeleteFamily(
                        rowKey2, colFamily, Long.MAX_VALUE)));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey.getBytes(), deleteCellMutation),
                KV.of(rowKey2.getBytes(), deleteColFamilyMutation)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(HbaseUtils.getRowResult(table, rowKey).isEmpty());
    Assert.assertTrue(HbaseUtils.getRowResult(table, rowKey2).isEmpty());
  }

  @Test
  public void writesDeletesThenPutsInOrderByTimestamp() throws Exception {
    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    // RowMutations entry ordering does not guarantee mutation ordering, as Hbase operations
    // are ordered by timestamp. See https://issues.apache.org/jira/browse/HBASE-2256
    RowMutations putDeletePut =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey, colFamily, colQualifier, value, timeT),
                    HbaseUtils.HbaseMutationBuilder.createDeleteFamily(
                        rowKey, colFamily, timeT + 1),
                    HbaseUtils.HbaseMutationBuilder.createPut(
                        rowKey, colFamily, colQualifier, value2, timeT + 2)));

    pipeline
        .apply("Create row mutations", Create.of(KV.of(rowKey.getBytes(), putDeletePut)))
        // KV.of(rowKey.getBytes(), delete),
        // KV.of(rowKey.getBytes(), put2)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily, colQualifier));
  }
}
