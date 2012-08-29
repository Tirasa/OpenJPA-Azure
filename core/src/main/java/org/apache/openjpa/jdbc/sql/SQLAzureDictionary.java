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

import org.apache.openjpa.utils.SQLAzureUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.federation.jdbc.FederationConfiguration;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.PrimaryKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.schema.Unique;

/**
 * Dictionary for Microsoft SQL Server.
 */
public class SQLAzureDictionary extends SQLServerDictionary {

    @Override
    public String[] getCreateTableSQL(Table table) {
        // -------------------------
        // just for check configuration parameters
        // -------------------------
        conf.getLog(OpenJPAConfiguration.LOG_RUNTIME).info(
                "Retrieve federations for " + this.getClass().getSimpleName());
        final String[] federations = ((FederationConfiguration) conf).getFederationNames();
        for (String federation : federations) {
            conf.getLog(OpenJPAConfiguration.LOG_RUNTIME).info("Federation " + federation);
        }
        // -------------------------

        final List<String> toBeCreated = new ArrayList<String>();

        DataSource ds = conf.getDataSource2(null);
        if (ds != null) {
            try {
                Connection conn = ds.getConnection();

                if (SQLAzureUtils.federation == null) {
                    toBeCreated.addAll(getStatement(table, null));
                } else {
                    if (!SQLAzureUtils.tableExists(conn, table, 0L)) {
                        toBeCreated.addAll(getStatement(table, 0L));
                    }

                    for (Long id : SQLAzureUtils.getMemberDistribution(conn)) {
                        // perform use federation and create table
                        if (id != null && id.longValue() != 0L && !SQLAzureUtils.tableExists(conn, table, id)) {
                            toBeCreated.addAll(getStatement(table, id));
                        }
                    }
                }
            } catch (SQLException ex) {
                conf.getLog("SQLAzure").error("Error creating schema", ex);
            }
        }

        return toBeCreated.toArray(new String[toBeCreated.size()]);
    }

    private List<String> getStatement(Table table, Long id) {
        final List<String> toBeCreated = new ArrayList<String>();

        if (id != null) {
            toBeCreated.add(
                    "USE FEDERATION " + SQLAzureUtils.federation + " (range_id=" + id + ") WITH FILTERING = OFF, RESET");
        }

        StringBuilder buf = new StringBuilder();

        String tableName = checkNameLength(
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

        // do this before getting the columns so we know how to handle
        // the last comma
        StringBuilder endBuf = new StringBuilder();
        PrimaryKey pk = table.getPrimaryKey();
        String pkStr;
        if (pk != null) {
            pkStr = getPrimaryKeyConstraintSQL(pk);
            if (pkStr != null) {
                endBuf.append(pkStr);
            }
        }

        Unique[] unqs = table.getUniques();
        String unqStr;
        for (int i = 0; i < unqs.length; i++) {
            unqStr = getUniqueConstraintSQL(unqs[i]);
            if (unqStr != null) {
                if (endBuf.length() > 0) {
                    endBuf.append(", ");
                }
                endBuf.append(unqStr);
            }
        }

        Column[] cols = table.getColumns();
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

        if (id != null) {
            buf.append("FEDERATED ON (range_id = id)");
        }

        toBeCreated.add(buf.toString());

        if (pk == null || pk.getColumns() == null || pk.getColumns().length == 0) {
            toBeCreated.add(
                    "CREATE CLUSTERED INDEX " + tableName + "_cindex ON " + tableName
                    + "(" + getNamingUtil().appendColumns(cols) + ")");
        }

        return toBeCreated;
    }
}
