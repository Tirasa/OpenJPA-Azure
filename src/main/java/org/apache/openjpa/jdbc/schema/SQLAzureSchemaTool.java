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
package org.apache.openjpa.jdbc.schema;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.apache.openjpa.federation.jdbc.Federation;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.sql.SQLAzureDictionary;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzureSchemaTool extends SchemaTool {

    private final Log _log;

    private final JDBCConfiguration _conf;

    private final DataSource _ds;

    private final SQLAzureDictionary _dict;

    private String _sqlTerminator = ";";

    private static final Localizer _loc = Localizer.forPackage(SQLAzureSchemaTool.class);

    public SQLAzureSchemaTool(final JDBCConfiguration conf, final String action) {
        super(conf, action);
        this._conf = conf;
        this._ds = conf.getDataSource2(null);
        this._dict = (SQLAzureDictionary) conf.getDBDictionaryInstance();
        this._log = conf.getLog(JDBCConfiguration.LOG_SCHEMA);
    }

    public SQLAzureSchemaTool(final JDBCConfiguration conf) {
        this(conf, null);
    }

    @Override
    public boolean createTable(final Table table)
            throws SQLException {

        final List<Federation> federations =
                ((SQLAzureConfiguration) _conf).getFederations(table.getFullIdentifier().getName());

        boolean res = true;

        if (federations.isEmpty()) {
            res = super.createTable(table);
        } else {
            final Connection conn = _ds.getConnection();
            try {
                for (Federation federation : federations) {
                    for (Object memberId : SQLAzureUtils.getMemberDistribution(conn, federation)) {
                        SQLAzureUtils.useFederation(conn, federation, memberId);
                        if (!SQLAzureUtils.tableExists(conn, table)) {
                            res &= executeSQL(_dict.getCreateTableSQL(table, federation), conn);
                        }
                    }
                }
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
    public boolean addForeignKey(ForeignKey fk)
            throws SQLException {
        final List<Federation> federations =
                ((SQLAzureConfiguration) _conf).getFederations(fk.getPrimaryKeyTable().getFullIdentifier().getName());

        boolean res = true;

        if (federations.isEmpty()) {
            res = super.addForeignKey(fk);
        }

        return res;
    }

    private boolean executeSQL(String[] sql, final Connection conn)
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

    private void handleException(SQLException sql)
            throws SQLException {

        if (!getIgnoreErrors()) {
            throw sql;
        }

        _log.warn(sql.getMessage(), sql);
    }

    @Override
    public void setSQLTerminator(String t) {
        _sqlTerminator = t;
    }
}
