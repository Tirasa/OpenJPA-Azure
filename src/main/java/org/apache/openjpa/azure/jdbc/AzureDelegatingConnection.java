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
package org.apache.openjpa.azure.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.slice.jdbc.DistributedConnection;
import org.apache.openjpa.slice.jdbc.DistributedPreparedStatement;

public class AzureDelegatingConnection extends DistributedConnection {

    private final AzureConfiguration conf;

    private final DataSource ds;

    private final Map<String, Connection> availableFedConnections = new HashMap<String, Connection>();

    private final List<Connection> openedConnections;

    private final List<Connection> workingConnections = new ArrayList<Connection>();

    private int workingIndex;

    // root connection
    private final Connection conn;

    public AzureDelegatingConnection(
            final List<Connection> connections, final DataSource ds, final AzureConfiguration conf)
            throws SQLException {

        super(connections);

        this.conf = conf;
        this.ds = ds;

        // get root connection
        conn = connections.get(0);

        // add root connection to the opened connection
        this.openedConnections = connections;

        workingConnections.addAll(connections);
        workingIndex = workingConnections.size();
    }

    @Override
    public PreparedStatement prepareStatement(final String sql)
            throws SQLException {

        final DistributedPreparedStatement ret = new AzurePreparedStatement(this);

        for (Connection c : workingConnections) {
            ret.add(c.prepareStatement(sql));
        }

        return ret;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int rsType, final int rsConcur)
            throws SQLException {

        final DistributedPreparedStatement ret = new AzurePreparedStatement(this);

        for (Connection c : workingConnections) {
            ret.add(c.prepareStatement(sql, rsType, rsConcur));
        }

        return ret;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] arg1)
            throws SQLException {

        final DistributedPreparedStatement ret = new AzurePreparedStatement(this);

        for (Connection c : workingConnections) {
            ret.add(c.prepareStatement(sql, arg1));
        }

        return ret;
    }

    @Override
    public void commit()
            throws SQLException {

        for (Connection c : openedConnections) {
            c.commit();
        }

        conn.commit();
    }

    @Override
    public void close()
            throws SQLException {
        for (Connection c : openedConnections) {
            c.close();
        }

        conn.close();

        openedConnections.clear();
        workingConnections.clear();
    }

    @Override
    public void rollback()
            throws SQLException {
        for (Connection c : openedConnections) {
            c.rollback();
        }

//        conn.rollback();
    }

    @Override
    public void rollback(Savepoint svptn)
            throws SQLException {
        for (Connection c : openedConnections) {
            c.rollback(svptn);
        }

//        conn.rollback(svptn);
    }

    public int getWorkingIndex() {
        return workingIndex;
    }
}
