package com.google.cloud.bigtable.data.v2.models;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.teleport.v2.templates.constants.TestConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

/**
 * Builder class for Bigtable change stream mutations. Allows change stream mutations to be built
 * in a simple manner.
 *
 * <p>ChangeStreamMutation mutation = TestChangeStreamMutation("row-key-1", ts) .put(...)
 * .build();
 */
@InternalApi("For internal testing only.")
public class ChangeStreamMutationBuilder {

    private ChangeStreamMutation.Builder builder;

    public ChangeStreamMutationBuilder(String rowKey, long atTimestamp) {
      Timestamp ts =
          Timestamp.newBuilder()
              .setSeconds(atTimestamp / 1000)
              .setNanos((int) ((atTimestamp % 1000) * 1000000))
              .build();
      this.builder =
          ChangeStreamMutation.createUserMutation(
                  ByteString.copyFromUtf8(rowKey),
                  TestConstants.testCluster,
                  ts,
                  0 // Multi-master transactions are not supported so tiebreaker set to 0.
                  )
              .setToken(TestConstants.testToken)
              .setLowWatermark(ts);
    }

    public ChangeStreamMutationBuilder setCell(
        String colFamily, String colQualifier, String value, long atTimestamp) {
      this.builder.setCell(
          colFamily,
          ByteString.copyFromUtf8(colQualifier),
          atTimestamp,
          ByteString.copyFromUtf8(value));
      return this;
    }

    public ChangeStreamMutationBuilder deleteCells(
        String colFamily, String colQualifier, long startCloseTimestamp, long endOpenMsTimestamp) {
      this.builder.deleteCells(
          colFamily,
          ByteString.copyFromUtf8(colQualifier),
          TimestampRange.create(startCloseTimestamp, endOpenMsTimestamp));
      return this;
    }

    public ChangeStreamMutationBuilder deleteFamily(String colFamily) {
      this.builder.deleteFamily(colFamily);
      return this;
    }

    public ChangeStreamMutation build() {
      return this.builder.build();
    }

}
