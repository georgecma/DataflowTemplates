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
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value2;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigtable.StaticBigtableResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.BigtableToHbasePipeline.BigtableToHbasePipelineOptions;
import com.google.cloud.teleport.v2.templates.utils.HbaseUtils;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End to end table that runs the pipeline from Bigtable to Hbase. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToHbasePipeline.class)
@RunWith(JUnit4.class)
public class BigtableToHbasePipelineIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableToHbasePipelineIT.class);

  // private static StaticBigtableResourceManager bigtableResourceManager;
  private static BigtableToHbasePipelineOptions pipelineOptions;
  private static HBaseTestingUtility hBaseTestingUtility;
  private static Table hbaseTable;
  private static StaticBigtableResourceManager bigtableResourceManager;
  private static String cbtQualifier;
  private static String hbaseQualifier;
  // Timeout for pipeline and tests
  private static int testTimeoutSeconds = 30;

  @BeforeClass
  public static void setUpCluster() throws Exception {

    // TODO: remove these params after Cdc GA
    // Parse input as though from a live run. This requires passing in params to test via
    // -Dparameters="..."
    String input = System.getProperty("parameters");
    String[] keyValuePairs = input.split(",");
    Map<String, String> args = new HashMap<>();
    for (String pair : keyValuePairs) {
      String[] entry = pair.split("=");
      args.put(entry[0], entry[1]);
    }
    hbaseQualifier = args.get("hbaseQualifier");
    cbtQualifier = args.get("cbtQualifier");

    // Create some pipeline options from args
    pipelineOptions = PipelineOptionsFactory.create().as(BigtableToHbasePipelineOptions.class);
    // Set bigtable change stream options
    pipelineOptions.setBigtableProjectId(args.get("bigtableProjectId"));
    pipelineOptions.setInstanceId(args.get("instanceId"));
    pipelineOptions.setTableId(args.get("tableId"));
    pipelineOptions.setAppProfileId(args.get("appProfileId"));
    // Set bidirectional replication options
    pipelineOptions.setBidirectionalReplicationEnabled(
        Boolean.parseBoolean(args.get("bidirectionalReplicationEnabled")));
    pipelineOptions.setCbtQualifier(cbtQualifier);
    pipelineOptions.setHbaseQualifier(hbaseQualifier);

    // Create Hbase cluster
    hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniCluster();
    // Create HBase table that mirrors persistent Hbase table.
    hbaseTable = HbaseUtils.createTable(hBaseTestingUtility, pipelineOptions.getTableId());
  }

  @Before
  public void setUp() throws Exception {
    // TODO: StaticBigtableResourceManager has to be replaced with DefaultBigtableResourceManager
    //  when it supports CDC configs
    // Create Bigtable resource manager in setUp because we need credentialsProvider which requires
    // non-static context
    bigtableResourceManager =
        StaticBigtableResourceManager.builder(pipelineOptions.getBigtableProjectId())
            .setCredentialsProvider(credentialsProvider)
            .setInstanceId(pipelineOptions.getInstanceId())
            .setTableId(pipelineOptions.getTableId())
            .setAppProfileId(pipelineOptions.getAppProfileId())
            .build();

    // Set time to just cover the timeframe of the upcoming test
    Timestamp start = Timestamp.now();
    Timestamp endTime =
        Timestamp.ofTimeSecondsAndNanos(start.getSeconds() + testTimeoutSeconds, start.getNanos());
    pipelineOptions.setStartTimestamp(start.toString());
    pipelineOptions.setEndTimestamp(endTime.toString());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (hBaseTestingUtility != null) {
      hBaseTestingUtility.shutdownMiniCluster();
    }
    if (bigtableResourceManager != null) {
      bigtableResourceManager.cleanupAll();
    }
  }

  @After
  public void cleanTable() throws IOException {
    // Clear bigtable table
    RowMutation deleteFamilies =
        RowMutation.create(pipelineOptions.getTableId(), rowKey).deleteFamily(colFamily);
    bigtableResourceManager.write(deleteFamilies);

    // Clear hbase table
    long now = Time.now();
    hbaseTable.delete(HbaseUtils.HbaseMutationBuilder.createDeleteFamily(rowKey, colFamily, now));
  }

  @Test
  public void testPutPipeline() throws Exception {
    // Write to Bigtable.
    RowMutation setCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, value);
    bigtableResourceManager.write(setCell);

    PipelineResult pipelineResult =
        BigtableToHbasePipeline.bigtableToHbasePipeline(
            pipelineOptions, hBaseTestingUtility.getConfiguration());

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }

    Assert.assertEquals(value, HbaseUtils.getCell(hbaseTable, rowKey, colFamily, colQualifier));
  }

  @Test
  public void testPutDeletePipeline() throws Exception {
    // Write to Bigtable.
    RowMutation setCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, Time.now() * 1000, value);

    RowMutation deleteCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .deleteCells(
                colFamily,
                ByteString.copyFromUtf8(colQualifier),
                TimestampRange.create(0L, (Time.now() + 2) * 1000));
    bigtableResourceManager.write(setCell);
    bigtableResourceManager.write(deleteCell);

    PipelineResult pipelineResult =
        BigtableToHbasePipeline.bigtableToHbasePipeline(
            pipelineOptions, hBaseTestingUtility.getConfiguration());

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }

    Assert.assertTrue(HbaseUtils.getRowResult(hbaseTable, rowKey).isEmpty());
  }

  @Test
  public void testBidirectionalReplicationFiltersEntries() throws Exception {
    // Write to Bigtable.
    RowMutation setCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, value);
    // Mimic Hbase-replicated entry that should be filtered out
    RowMutation setCellToBeFiltered =
        RowMutation.create(pipelineOptions.getTableId(), rowKey2)
            .setCell(colFamily, colQualifier, value)
            .deleteCells(colFamily, hbaseQualifier);
    bigtableResourceManager.write(setCell);
    bigtableResourceManager.write(setCellToBeFiltered);

    PipelineResult pipelineResult =
        BigtableToHbasePipeline.bigtableToHbasePipeline(
            pipelineOptions, hBaseTestingUtility.getConfiguration());

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }

    Assert.assertEquals(value, HbaseUtils.getCell(hbaseTable, rowKey, colFamily, colQualifier));
    Assert.assertTrue(HbaseUtils.getRowResult(hbaseTable, rowKey2).isEmpty());
  }

  /**
   * We cannot guarantee Dataflow will apply mutations in-order, esp. when Hbase does retries, here
   * we simulate such an out of order write by setting staggered timestamp and writing the mutations
   * out of order. Since Hbase determines everything by timestamp, we expect that Hbase should be
   * able to handle this scenario.
   *
   * @throws Exception
   */
  @Test
  public void testPutPipelineOutOfOrder() throws Exception {
    // Write put,delete,put to Bigtable out of order.
    RowMutation setCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, (Time.now() - 2) * 1000, value);

    RowMutation deleteCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .deleteCells(
                colFamily,
                ByteString.copyFromUtf8(colQualifier),
                TimestampRange.create(0L, (Time.now() - 1) * 1000));

    RowMutation secondSetCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, Time.now() * 1000, value2);

    bigtableResourceManager.write(secondSetCell);
    bigtableResourceManager.write(deleteCell);
    bigtableResourceManager.write(setCell);

    PipelineResult pipelineResult =
        BigtableToHbasePipeline.bigtableToHbasePipeline(
            pipelineOptions, hBaseTestingUtility.getConfiguration());

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }

    Assert.assertEquals(value2, HbaseUtils.getCell(hbaseTable, rowKey, colFamily, colQualifier));
  }

  @Test
  public void testHbaseTableDisabled() throws Exception, IOException {
    RowMutation setCell =
        RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, value);

    // Start pipeline
    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(
        () -> {
          PipelineResult pipelineResult =
              BigtableToHbasePipeline.bigtableToHbasePipeline(
                  pipelineOptions, hBaseTestingUtility.getConfiguration());
          pipelineResult.waitUntilFinish();
        });
    // Write cell, disable downstream table, pipeline should retry until table is up again
    executor.execute(
        () -> {
          try {
            hBaseTestingUtility.getAdmin().disableTable(hbaseTable.getName());
            bigtableResourceManager.write(setCell);
            hBaseTestingUtility.getAdmin().enableTable(hbaseTable.getName());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    executor.awaitTermination(testTimeoutSeconds, TimeUnit.SECONDS);

    Assert.assertEquals(value, HbaseUtils.getCell(hbaseTable, rowKey, colFamily, colQualifier));
  }
}
