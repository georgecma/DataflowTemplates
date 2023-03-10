package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.templates.transforms.HbaseRowMutationIO;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assumes one connection.
 * TODO: verify that only one connection is ever needed, i.e. we won't need a shared cache.
 */
public class HbaseConnectionDao implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HbaseConnectionDao.class);
  private static final long serialVersionUID = 5252999807656940415L;

  // Transient connection to be initialized per worker
  private transient Connection connection;
  // Number of threads using the shared connection, close connection if connectionCount goes to 0
  private int connectionCount;

  public HbaseConnectionDao() {}

  public String getDebugString() {
    return String.format(
        "Connection status: %s\n" + "Connectors: %s\n",
        this.connection != null,
        connectionCount
        );
  }

  public int getConnectionCount() {
    return this.connectionCount;
  }

  /**
   * Creates a Hbase connection if it doesn't exist. If it does exist, return it.
   */
  public synchronized Connection getOrCreate(Configuration configuration
  ) throws IOException {
    if (this.connection == null) {
      this.connection = ConnectionFactory.createConnection(configuration);
    }
    connectionCount++;
    Metrics.counter(HbaseConnectionDao.class, "active_hbase_connection").inc();

    return this.connection;
  }

  public synchronized void close() throws IOException {
    connectionCount--;
    Metrics.counter(HbaseConnectionDao.class, "active_hbase_connection").dec();
    if (connectionCount == 0 && this.connection != null) {
      this.connection.close();
    }
    if (connectionCount < 0) {
      LOG.warn("Connection count at " + connectionCount + ", should not be possible");
    }
  }
}
