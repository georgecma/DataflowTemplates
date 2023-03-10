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

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assumes one connection. TODO: verify that only one connection is ever needed, i.e. we won't need
 * a shared cache.
 */
public class HbaseSharedConnection implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HbaseSharedConnection.class);
  private static final long serialVersionUID = 5252999807656940415L;

  private static HbaseConnection hbaseConnection;

  public static synchronized Connection getOrCreate(Configuration configuration)
      throws IOException {
    if (hbaseConnection == null) {
      hbaseConnection = new HbaseConnection();
    }
    return hbaseConnection.getOrCreate(configuration);
  }

  public synchronized void close() throws IOException {
    if (hbaseConnection != null) {
      hbaseConnection.close();
      hbaseConnection = null;
    }
  }

  private static class HbaseConnection implements Serializable {
    // Transient connection to be initialized per worker
    private transient Connection connection;
    // Number of threads using the shared connection, close connection if connectionCount goes to 0
    private int connectionCount;

    public synchronized Connection getOrCreate(Configuration configuration) throws IOException {
      if (connection == null || connection.isClosed()) {
        connection = ConnectionFactory.createConnection(configuration);
      }
      connectionCount++;
      return connection;
    }

    public synchronized void close() throws IOException {
      connectionCount--;
      if (connectionCount == 0 && connection != null) {
        connection.close();
        connection = null;
      }
      if (connectionCount < 0) {
        LOG.warn(
            "Connection count at " + hbaseConnection.connectionCount + ", should not be possible");
      }
    }

    public String getDebugString() {
      return String.format(
          "Connection status: %s\n" + "Connectors: %s\n", this.connection != null, connectionCount);
    }

    public int getConnectionCount() {
      return this.connectionCount;
    }
  }
}
