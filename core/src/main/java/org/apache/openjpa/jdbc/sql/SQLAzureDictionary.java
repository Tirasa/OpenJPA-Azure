/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.openjpa.jdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.federation.jdbc.FederationConfiguration;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.PrimaryKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.schema.Unique;
import org.apache.openjpa.utils.SQLAzureUtils;

/**
 * Dictionary for Microsoft SQL Server.
 */
public class SQLAzureDictionary extends SQLServerDictionary {

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getCreateTableSQL(final Table table) {

        final List<String> toBeCreated = new ArrayList<String>();

        DataSource ds = conf.getDataSource2(null);

        if (ds != null) {
            Connection conn = null;
            try {
                conn = ds.getConnection();

                if (((FederationConfiguration) conf).isFederated()) {
                    for (String id : SQLAzureUtils.getMemberDistribution(conn, (FederationConfiguration) conf)) {
                        // perform use federation and create table
                        if (!SQLAzureUtils.tableExists(conn, table, id)) {
                            toBeCreated.addAll(getStatements(table, id));
                        }
                    }
                } else {
                    toBeCreated.addAll(getStatements(table));
                }

            } catch (SQLException ex) {
                conf.getLog("SQLAzure").error("Error creating schema", ex);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.error("Error closing connection", e);
                    }
                }
            }
        }

        return toBeCreated.toArray(new String[toBeCreated.size()]);
    }

    /**
     * Get SQL statement needed to create a federated table.
     *
     * @param table table to be created.
     * @param id ragge id;
     * @return list of statements.
     */
    private List<String> getStatements(final Table table, final Object id) {

        final List<String> toBeCreated = new ArrayList<String>();

        toBeCreated.add("USE FEDERATION " + SQLAzureUtils.federation
                + " (range_id=" + id + ") WITH FILTERING = OFF, RESET");

        final List<String> stms = getStatements(table);

        toBeCreated.add(stms.get(0)
                + " FEDERATED ON (range_id = " + ((FederationConfiguration) conf).getRangeMappingName() + ")");

        for (String stm : stms.subList(1, stms.size())) {
            toBeCreated.add(stm);
        }

        return toBeCreated;
    }

    /**
     * Get SQL statement needed to create a non federated table.
     *
     * @param table table to be created.
     * @return list of statements.
     */
    private List<String> getStatements(final Table table) {
        final Map.Entry<String, String> statement = getCreateTableStm(table);

        final List<String> toBeCreated = new ArrayList<String>();

        toBeCreated.add(statement.getValue());

        final PrimaryKey primaryKey = table.getPrimaryKey();

        // TODO: cluster index creation have to be verified
        if (primaryKey == null || primaryKey.getColumns() == null || primaryKey.getColumns().length == 0) {

            toBeCreated.add("CREATE CLUSTERED INDEX " + statement.getKey() + "_cindex ON " + statement.getKey()
                    + "(" + getNamingUtil().appendColumns(table.getColumns()) + ")");
        }

        return toBeCreated;
    }

    /**
     * Create a standard SQL statement for table creation.
     *
     * @param table table to be created.
     * @return the value pair "table name" / "SQL statement".
     */
    private Map.Entry<String, String> getCreateTableStm(final Table table) {

        final StringBuilder buf = new StringBuilder();

        final String tableName = checkNameLength(
                getFullIdentifier(table, false),
                maxTableNameLength,
                "long-table-name",
                tableLengthIncludesSchema);

        buf.append("CREATE TABLE ").append(tableName);

        if (supportsComments && table.hasComment()) {
            buf.append(" ");
            comment(buf, table.getComment());
            buf.append("\n    (");
        } else {
            buf.append(" (");
        }

        final StringBuilder endBuf = new StringBuilder();
        final PrimaryKey primaryKey = table.getPrimaryKey();

        if (primaryKey != null) {
            final String pkStr = getPrimaryKeyConstraintSQL(primaryKey);
            if (StringUtils.isNotBlank(pkStr)) {
                endBuf.append(pkStr);
            }
        }

        final Unique[] unqs = table.getUniques();

        for (int i = 0; i < unqs.length; i++) {
            final String unqStr = getUniqueConstraintSQL(unqs[i]);
            if (StringUtils.isNotBlank(unqStr)) {
                if (endBuf.length() > 0) {
                    endBuf.append(", ");
                }
                endBuf.append(unqStr);
            }
        }

        final Column[] cols = table.getColumns();
        for (int i = 0; i < cols.length; i++) {
            buf.append(getDeclareColumnSQL(cols[i], false));
            if (i < cols.length - 1 || endBuf.length() > 0) {
                buf.append(", ");
            }
            if (supportsComments && cols[i].hasComment()) {
                comment(buf, cols[i].getComment());
                buf.append("\n    ");
            }
        }

        buf.append(endBuf.toString());
        buf.append(")");

        return new AbstractMap.SimpleEntry<String, String>(tableName, buf.toString());
    }
}
