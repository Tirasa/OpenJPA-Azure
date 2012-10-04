/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.openjpa.jdbc.kernel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.kernel.StoreQuery;

public class AzureStoreManager extends JDBCStoreManager {

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {

        final List<Connection> connections = new ArrayList<Connection>();
        connections.add(getDataSource().getConnection());

        final AzureDelegatingConnection conn = new AzureDelegatingConnection(
                connections, getDataSource(), (AzureConfiguration) getConfiguration());

        return new AzureRefCountConnection(conn);
    }

    public class AzureRefCountConnection extends RefCountConnection {

        private final Connection conn;

        public AzureRefCountConnection(final Connection conn) {
            super(conn);
            this.conn = conn;
        }

        public Connection getConn() {
            return conn;
        }
    }

    @Override
    public StoreQuery newQuery(final String language) {
        ((AzureDelegatingConnection) ((AzureRefCountConnection) getConnection()).getConn()).
                selectWorkingConnections();

        return super.newQuery(language);
    }
}
