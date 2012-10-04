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
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.sql.Row;
import org.apache.openjpa.jdbc.sql.RowImpl;
import org.apache.openjpa.slice.jdbc.DistributedConnection;
import org.apache.openjpa.slice.jdbc.DistributedPreparedStatement;
import org.apache.openjpa.azure.util.MemberDistribution;
import org.apache.openjpa.azure.util.AzureUtils;

public class AzureDelegatingConnection extends DistributedConnection {

    private final AzureConfiguration conf;

    private final DataSource ds;

    private final Map<String, Connection> availableFedConnections = new HashMap<String, Connection>();

    private final List<Connection> openedConnections;

    private final List<Connection> workingConnections = new ArrayList<Connection>();

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

        // the root is the default connection
        workingConnections.add(conn);
    }

    public void selectWorkingConnections() {
        workingConnections.clear();

        try {

            // add federation connections
            for (Federation federation : conf.getFederations()) {
                for (Object memberId : AzureUtils.getMemberDistribution(this.conn, federation)) {
                    final String memberKey = federation.getName() + ":" + AzureUtils.getObjectIdAsString(memberId);

                    final Connection conn;

                    if (availableFedConnections.containsKey(memberKey)) {
                        conn = availableFedConnections.get(memberKey);
                    } else {
                        conn = ds.getConnection();
                        conn.setAutoCommit(this.conn.getAutoCommit());
                        AzureUtils.useFederation(conn, federation, memberId);
                        availableFedConnections.put(memberKey, conn);
                        openedConnections.add(conn);
                    }

                    workingConnections.add(conn);
                }
            }

        } catch (SQLException e) {
            conf.getLog(OpenJPAConfiguration.LOG_RUNTIME).error("Error connecting to the database", e);
        }

        if (workingConnections.isEmpty()) {
            workingConnections.add(this.conn);
        }
    }

    public void selectWorkingConnections(final RowImpl row) {
        workingConnections.clear();

        final Table table = row.getTable();

        final List<Federation> federations = conf.getFederations(table.getFullIdentifier().getName());

        // TODO: Currently we cannot store a table on the root and an a federation at the same time. 
        //       This behavior should be changed.
        try {
            // get federation connections
            for (Federation federation : federations) {
                MemberDistribution memberDistribution;

                if (row != null && row.getAction() == Row.ACTION_INSERT) {
                    final String rangeMappingName = federation.getRangeMappingName(table.getFullIdentifier().getName());
                    final Column col = table.getColumn(DBIdentifier.newColumn(rangeMappingName), false);

                    memberDistribution = new MemberDistribution(federation.getRangeMappingType());

                    memberDistribution.addValue(
                            AzureUtils.getMemberDistribution(conn, federation, row.getVals()[col.getIndex()]));
                } else {
                    memberDistribution = AzureUtils.getMemberDistribution(this.conn, federation);
                }

                for (Object memberId : memberDistribution) {
                    final String memberKey = federation.getName() + ":" + AzureUtils.getObjectIdAsString(memberId);

                    final Connection conn;

                    if (availableFedConnections.containsKey(memberKey)) {
                        conn = availableFedConnections.get(memberKey);
                    } else {
                        conn = ds.getConnection();
                        conn.setAutoCommit(this.conn.getAutoCommit());
                        AzureUtils.useFederation(conn, federation, memberId);
                        availableFedConnections.put(memberKey, conn);
                        openedConnections.add(conn);
                    }

                    workingConnections.add(conn);
                }
            }
        } catch (SQLException e) {
            conf.getLog(OpenJPAConfiguration.LOG_RUNTIME).error("Error connecting to the database", e);
        }

        if (workingConnections.isEmpty()) {
            workingConnections.add(this.conn);
        }
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
}
