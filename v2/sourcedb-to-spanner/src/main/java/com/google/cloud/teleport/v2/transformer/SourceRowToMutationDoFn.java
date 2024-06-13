/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transformer;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.templates.RowContext;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a DoFn class that takes a {@link SourceRow} and converts it into a {@link
 * com.google.cloud.spanner.Mutation}.
 */
@AutoValue
public abstract class SourceRowToMutationDoFn extends DoFn<SourceRow, RowContext>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRowToMutationDoFn.class);

  private final Counter transformerErrors =
      Metrics.counter(SourceRowToMutationDoFn.class, MetricCounters.TRANSFORMER_ERRORS);

  public abstract ISchemaMapper iSchemaMapper();

  public abstract Map<String, SourceTableReference> tableIdMapper();

  public static SourceRowToMutationDoFn create(
      ISchemaMapper iSchemaMapper, Map<String, SourceTableReference> tableIdMapper) {
    return new AutoValue_SourceRowToMutationDoFn(iSchemaMapper, tableIdMapper);
  }

  @ProcessElement
  public void processElement(ProcessContext c, MultiOutputReceiver output) {
    SourceRow sourceRow = c.element();
    LOG.debug("Starting transformation for Source Row {}", sourceRow);

    if (!tableIdMapper().containsKey(sourceRow.tableSchemaUUID())) {
      // TODO: Remove LOG statements from processElement once counters and DLQ is supported.
      // TODO: Add metric for ignored rows
      LOG.error(
          "cannot find valid sourceTable for tableId: {} in tableIdMapper",
          sourceRow.tableSchemaUUID());
      transformerErrors.inc();
      return;
    }
    try {
      // TODO: update namespace in constructor when Spanner namespace support is added.
      GenericRecord record = sourceRow.getPayload();
      String srcTableName = tableIdMapper().get(sourceRow.tableSchemaUUID()).sourceTableName();
      GenericRecordTypeConvertor genericRecordTypeConvertor =
          new GenericRecordTypeConvertor(iSchemaMapper(), "", sourceRow.shardId());
      Map<String, Value> values =
          genericRecordTypeConvertor.transformChangeEvent(record, srcTableName);
      String spannerTableName = iSchemaMapper().getSpannerTableName("", srcTableName);
      // TODO: Move the mutation generation to writer. Create generic record here instead
      Mutation mutation = mutationFromMap(spannerTableName, values);
      output
          .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
          .output(RowContext.builder().setRow(sourceRow).setMutation(mutation).build());
    } catch (Exception e) {
      transformerErrors.inc();
      output
          .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
          .output(RowContext.builder().setRow(sourceRow).setErr(e).build());
    }
  }

  private Mutation mutationFromMap(String spannerTableName, Map<String, Value> values) {
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(spannerTableName);
    for (String spannerColName : values.keySet()) {
      Value value = values.get(spannerColName);
      if (value != null) {
        builder.set(spannerColName).to(value);
      }
    }
    return builder.build();
  }
}
