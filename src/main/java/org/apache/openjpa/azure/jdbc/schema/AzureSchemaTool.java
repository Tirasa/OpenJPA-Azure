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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.openjpa.azure.AzureSliceConfiguration;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.AzureSliceStoreManager;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.Index;
import org.apache.openjpa.jdbc.schema.Schema;
import org.apache.openjpa.jdbc.schema.SchemaGroup;
import org.apache.openjpa.jdbc.schema.SchemaTool;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.schema.Unique;
import org.apache.openjpa.jdbc.sql.AzureDictionary;

public class AzureSchemaTool extends SchemaTool {

    private final List<String> managedTables = new ArrayList<String>();

    private final JDBCConfiguration conf;

    public AzureSchemaTool(final JDBCConfiguration conf) {
        super(conf);
        this.conf = conf;
    }

    public AzureSchemaTool(final JDBCConfiguration conf, final String action) {
        super(conf, action);
        this.conf = conf;
    }

    @Override
    public boolean createTable(final Table table)
            throws SQLException {

        Map.Entry<Connection, Federation> conn = null;

        boolean res = true;

        try {
            conn = getConnection(table);

            if (conn != null) {
                if (!AzureUtils.tableExists(conn.getKey(), table)) {
                    res &= executeSQL(
                            ((AzureDictionary) _dict).getCreateTableSQL(table, conn.getValue()), conn.getKey());

                    if (res) {
                        managedTables.add(table.getFullIdentifier().getName());
                    }
                }

            }
        } finally {
            try {
                if (conn != null && conn.getKey() != null) {
                    conn.getKey().close();
                }
            } catch (SQLException ignore) {
                // ignore
            }
        }

        return res;
    }

    @Override
    public boolean addForeignKey(final ForeignKey fk)
            throws SQLException {

        return true;
//        return managedTables.contains(fk.getPrimaryKeyTable().getFullIdentifier().getName())
//                ? super.addForeignKey(fk) : true;
    }

    @Override
    public boolean createIndex(Index idx, Table table, Unique[] uniques)
            throws SQLException {
        // Informix will automatically create a unique index for the 
        // primary key, so don't create another index again

        if (!_dict.needsToCreateIndex(idx, table, uniques)) {
            return false;
        }

        int max = _dict.maxIndexesPerTable;

        int len = table.getIndexes().length;
        if (table.getPrimaryKey() != null) {
            len += table.getPrimaryKey().getColumns().length;
        }

        if (len >= max) {
            _log.warn(_loc.get("too-many-indexes", idx, table, max + ""));
            return false;
        }


        boolean res = true;

        if (managedTables.contains(table.getFullIdentifier().getName())) {
            Connection conn = null;

            try {
                conn = getConnection();
                res &= executeSQL(_dict.getCreateIndexSQL(idx), conn);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException ignore) {
                    // ignore
                }
            }
        }

        return res;
    }

    @Override
    public boolean dropTable(final Table table)
            throws SQLException {

        Map.Entry<Connection, Federation> conn = null;

        boolean result = true;

        try {
            conn = getConnection(table);

            if (conn != null && conn.getKey() != null) {
                if (AzureUtils.tableExists(conn.getKey(), table)) {
                    result &= executeSQL(_dict.getDropTableSQL(table), conn.getKey());
                }
            }

        } finally {
            try {
                if (conn != null && conn.getKey() != null) {
                    conn.getKey().close();
                }
            } catch (SQLException ignore) {
                // ignore
            }
        }

        return result;
    }

    @Override
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

            Map.Entry<Connection, Federation> conn = null;

            try {
                conn = getConnection(table);

                if (conn != null && conn.getKey() != null) {

                    if (AzureUtils.tableExists(conn.getKey(), table)) {
                        final String[] sql = _dict.getDeleteTableContentsSQL(tableArray, conn.getKey());
                        if (!executeSQL(sql, conn.getKey())) {
                            _log.warn(_loc.get("delete-table-contents"));
                        }
                    }
                }

            } finally {
                try {
                    if (conn != null && conn.getKey() != null) {
                        conn.getKey().close();
                    }
                } catch (SQLException ignore) {
                    // ignore
                }
            }
        }
    }

    private boolean executeSQL(final String[] sql, final Connection conn)
            throws SQLException {

        if (sql.length == 0) {
            return false;
        }

        final boolean wasAuto = conn.getAutoCommit();

        boolean err = false;

        try {
            if (getWriter() == null) {
                Statement stmt = null;

                if (!wasAuto) {
                    conn.setAutoCommit(true);
                }

                for (int i = 0; i < sql.length; i++) {
                    try {
                        try {
                            conn.rollback();
                        } catch (Exception e) {
                        }

                        stmt = conn.createStatement();
                        stmt.executeUpdate(sql[i]);

                        try {
                            conn.commit();
                        } catch (Exception e) {
                        }
                    } catch (SQLException se) {
                        err = true;
                        handleException(se);
                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
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

    private Map.Entry<Connection, Federation> getConnection(final Table table)
            throws SQLException {

        final List<Federation> federations = new ArrayList<Federation>();
        federations.addAll(((AzureSliceConfiguration) conf).getFederations(table.getFullIdentifier().getName()));

        boolean federated = !federations.isEmpty();

        for (ForeignKey fk : table.getForeignKeys()) {
            federations.addAll(((AzureSliceConfiguration) conf).getFederations(fk.getPrimaryKeyTable()));
        }

        final String sliceName = AzureUtils.getSliceName(conf.getValue("Id").get().toString());
        final Federation fed = ((AzureSliceConfiguration) conf).getFederation(sliceName);

        final Map.Entry<Connection, Federation> conn =
                ("ROOT".equals(sliceName) && federations.isEmpty()) || federations.contains(fed)
                ? new AbstractMap.SimpleEntry<Connection, Federation>(_ds.getConnection(), federated ? fed : null)
                : null;

        if (conn != null && fed != null) {
            if (AzureSliceStoreManager.federations == null || AzureSliceStoreManager.federations.isEmpty()) {
                AzureSliceStoreManager.initFederations(((AzureSliceConfiguration) conf).getGlobalConf(), conn.getKey());
            }

            int index = AzureUtils.getSliceMemberIndex(sliceName);

            AzureUtils.useFederation(conn.getKey(), fed, AzureSliceStoreManager.federations.get(fed).get(index));
        }

        return conn;
    }

    private Connection getConnection()
            throws SQLException {
        final Connection conn = _ds.getConnection();

        final String sliceName = AzureUtils.getSliceName(conf.getValue("Id").get().toString());
        final Federation fed = ((AzureSliceConfiguration) conf).getFederation(sliceName);

        if (conn != null && fed != null) {
            if (AzureSliceStoreManager.federations == null || AzureSliceStoreManager.federations.isEmpty()) {
                AzureSliceStoreManager.initFederations(((AzureSliceConfiguration) conf).getGlobalConf(), conn);
            }

            int index = AzureUtils.getSliceMemberIndex(sliceName);

            AzureUtils.useFederation(conn, fed, AzureSliceStoreManager.federations.get(fed).get(index));
        }

        return conn;
    }
}
