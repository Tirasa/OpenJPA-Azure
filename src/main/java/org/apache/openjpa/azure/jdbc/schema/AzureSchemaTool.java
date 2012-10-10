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
package org.apache.openjpa.azure.jdbc.schema;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.Schema;
import org.apache.openjpa.jdbc.schema.SchemaGroup;
import org.apache.openjpa.jdbc.schema.SchemaTool;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.sql.AzureDictionary;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.lib.util.Localizer;

public class AzureSchemaTool extends SchemaTool {

    private final Log _log;

    private final JDBCConfiguration _conf;

    private final DataSource _ds;

    private final AzureDictionary _dict;

    private String _sqlTerminator = ";";

    private static final Localizer _loc = Localizer.forPackage(SchemaTool.class);

    public AzureSchemaTool(final JDBCConfiguration conf, final String action) {
        super(conf, action);
        this._conf = conf;
        this._ds = conf.getDataSource2(null);
        this._dict = (AzureDictionary) conf.getDBDictionaryInstance();
        this._log = conf.getLog(JDBCConfiguration.LOG_SCHEMA);
    }

    public AzureSchemaTool(final JDBCConfiguration conf) {
        this(conf, null);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void run()
            throws SQLException {

        if (ACTION_DELETE_TABLE_CONTENTS.equals(getAction())) {
            deleteTableContents();
        } else {
            super.run();
        }
    }

    @Override
    public boolean createTable(final Table table)
            throws SQLException {

        final List<Federation> federations =
                ((AzureConfiguration) _conf).getFederations(table.getFullIdentifier().getName());

        boolean res = true;

        final Map<Connection, Federation> connections = getWorkingConnections(federations);

        for (Map.Entry<Connection, Federation> conn : connections.entrySet()) {
            try {
                if (!AzureUtils.tableExists(conn.getKey(), table)) {
                    res &= executeSQL(_dict.getCreateTableSQL(table, conn.getValue()), conn.getKey());
                }

            } finally {
                try {
                    conn.getKey().close();
                } catch (SQLException se) {
                }
            }
        }

        return res;
    }

    @Override
    public boolean addForeignKey(final ForeignKey fk)
            throws SQLException {

        final List<Federation> federations =
                ((AzureConfiguration) _conf).getFederations(fk.getPrimaryKeyTable().getFullIdentifier().getName());

        return federations.isEmpty() ? super.addForeignKey(fk) : true;
    }

    @Override
    public boolean dropTable(final Table table)
            throws SQLException {

        boolean result = true;

        final List<Federation> federations =
                ((AzureConfiguration) _conf).getFederations(table.getFullIdentifier().getName());

        final Map<Connection, Federation> connections = getWorkingConnections(federations);
        for (Map.Entry<Connection, Federation> conn : connections.entrySet()) {
            try {
                if (!AzureUtils.tableExists(conn.getKey(), table)) {
                    final String[] sql = _dict.getDropTableSQL(table);
                    result &= executeSQL(sql, conn.getKey());
                }
            } finally {
                try {
                    conn.getKey().close();
                } catch (SQLException se) {
                }
            }
        }

        return result;
    }

    /**
     * Issue DELETE statement against all known tables.
     */
    protected void deleteTableContents()
            throws SQLException {

        final SchemaGroup group = getSchemaGroup();
        final Schema[] schemas = group.getSchemas();
        final Collection<Table> tables = new LinkedHashSet<Table>();
        for (int i = 0; i < schemas.length; i++) {
            tables.addAll(Arrays.asList(schemas[i].getTables()));
        }

        for (Table table : tables) {
            final Table[] tableArray = new Table[]{table};

            final List<Federation> federations =
                    ((AzureConfiguration) _conf).getFederations(table.getFullIdentifier().getName());

            final Map<Connection, Federation> connections = getWorkingConnections(federations);
            for (Map.Entry<Connection, Federation> conn : connections.entrySet()) {
                try {
                    if (!AzureUtils.tableExists(conn.getKey(), table)) {
                        final String[] sql = _dict.getDeleteTableContentsSQL(tableArray, conn.getKey());
                        if (!executeSQL(sql, conn.getKey())) {
                            _log.warn(_loc.get("delete-table-contents"));
                        }
                    }
                } finally {
                    try {
                        conn.getKey().close();
                    } catch (SQLException se) {
                    }
                }
            }
        }
    }

    private boolean executeSQL(final String[] sql, final Connection conn)
            throws SQLException {

        if (sql.length == 0) {
            return false;
        }

        boolean wasAuto = conn.getAutoCommit();

        boolean err = false;

        try {
            if (getWriter() == null) {
                Statement statement = null;

                if (!wasAuto) {
                    conn.setAutoCommit(true);
                }

                for (int i = 0; i < sql.length; i++) {
                    try {
                        try {
                            conn.rollback();
                        } catch (Exception e) {
                        }

                        statement = conn.createStatement();
                        statement.executeUpdate(sql[i]);

                        try {
                            conn.commit();
                        } catch (Exception e) {
                        }
                    } catch (SQLException se) {
                        err = true;
                        handleException(se);
                    } finally {
                        if (statement != null) {
                            try {
                                statement.close();
                            } catch (SQLException se) {
                            }
                        }
                    }
                }
            } else {
                for (int i = 0; i < sql.length; i++) {
                    ((PrintWriter) getWriter()).println(sql[i] + _sqlTerminator);
                }
                ((PrintWriter) getWriter()).flush();
            }
        } finally {
            if (!wasAuto) {
                conn.setAutoCommit(false);
            }
        }

        return !err;
    }

    private Map<Connection, Federation> getWorkingConnections(final List<Federation> federations)
            throws SQLException {

        final Map<Connection, Federation> connections = new HashMap<Connection, Federation>();

        final Connection root = _ds.getConnection();

        if (federations.isEmpty()) {
            connections.put(root, null);
        } else {
            try {
                for (Federation federation : federations) {
                    for (Object memberId : AzureUtils.getMemberDistribution(root, federation)) {
                        Connection conn = _ds.getConnection();
                        AzureUtils.useFederation(conn, federation, memberId);
                        connections.put(conn, federation);
                    }
                }
            } finally {
                if (root != null) {
                    try {
                        root.close();
                    } catch (SQLException ignore) {
                        // ignore
                    }
                }
            }
        }

        return connections;
    }
}
