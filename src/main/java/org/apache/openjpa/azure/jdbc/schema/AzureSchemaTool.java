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
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfigurationImpl;
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
import org.apache.openjpa.slice.Slice;

public class AzureSchemaTool extends SchemaTool {

    private final Slice slice;

    private final AzureConfigurationImpl globalConf;

    private final List<String> managedTables = new ArrayList<String>();

    public AzureSchemaTool(final JDBCConfiguration conf) {
        super(conf);
        this.slice = null;
        this.globalConf = (AzureConfigurationImpl) conf;
    }

    public AzureSchemaTool(final Slice slice, final JDBCConfiguration conf, final String action) {
        super((JDBCConfiguration) slice.getConfiguration(), action);
        this.slice = slice;
        this.globalConf = (AzureConfigurationImpl) conf;
    }

    @Override
    public boolean createTable(final Table table)
            throws SQLException {

        final Map.Entry<Connection, Federation> conn = getConnection(table);

        boolean res = true;

        if (conn != null) {

            try {
                if (!AzureUtils.tableExists(conn.getKey(), table)) {
                    res &= executeSQL(
                            ((AzureDictionary) _dict).getCreateTableSQL(table, conn.getValue()), conn.getKey());

                    if (res) {
                        managedTables.add(table.getFullIdentifier().getName());
                    }
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
            final Connection conn = getConnection();

            try {
                res &= executeSQL(_dict.getCreateIndexSQL(idx), conn);
            } finally {
                try {
                    conn.close();
                } catch (SQLException se) {
                }
            }
        }

        return res;
    }

    @Override
    public boolean dropTable(final Table table)
            throws SQLException {

        final Map.Entry<Connection, Federation> conn = getConnection(table);

        boolean result = true;

        if (conn != null) {
            try {
                if (AzureUtils.tableExists(conn.getKey(), table)) {
                    result &= executeSQL(_dict.getDropTableSQL(table), conn.getKey());
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

            final Map.Entry<Connection, Federation> conn = getConnection(table);

            if (conn != null) {
                try {
                    if (AzureUtils.tableExists(conn.getKey(), table)) {
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
        federations.addAll(globalConf.getFederations(table));

        boolean federated = !globalConf.getFederations(table.getFullIdentifier().getName()).isEmpty();

        for (ForeignKey fk : table.getForeignKeys()) {
            federations.addAll(globalConf.getFederations(fk.getPrimaryKeyTable()));
        }

        final Federation fed = globalConf.getFederation(slice);

        Map.Entry<Connection, Federation> conn =
                ("ROOT".equals(slice.getName()) && federations.isEmpty()) || federations.contains(fed)
                ? new AbstractMap.SimpleEntry<Connection, Federation>(_ds.getConnection(), federated ? fed : null)
                : null;

        if (conn != null && fed != null) {
            AzureUtils.useFederation(conn.getKey(), fed);
        }

        return conn;
    }

    private Connection getConnection()
            throws SQLException {
        return AzureUtils.useFederation(_ds.getConnection(), globalConf.getFederation(slice));
    }
}
