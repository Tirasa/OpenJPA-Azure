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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.federation.jdbc.Federation;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.PrimaryKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.schema.Unique;
import org.apache.openjpa.utils.SQLAzureUtils;

/**
 * Dictionary for Microsoft SQL Server.
 */
public class SQLAzureDictionary extends SQLServerDictionary {

    @Override
    public Column[] getColumns(
            final DatabaseMetaData meta,
            final DBIdentifier catalog,
            final DBIdentifier schemaName,
            final DBIdentifier tableName,
            final DBIdentifier columnName,
            final Connection conn)
            throws SQLException {

        SQLAzureUtils.useRootFederation(conn);

        final Collection<Federation> federations = tableName == null
                ? ((SQLAzureConfiguration) conf).getFederations()
                : ((SQLAzureConfiguration) conf).getFederations(tableName.getName());

        Column[] columns = null;

        if (federations.isEmpty()) {
            columns = getColumns(conn, schemaName, tableName, columnName);
        } else {
            for (Federation federation : federations) {
                for (Object memberId : SQLAzureUtils.getMemberDistribution(conn, federation)) {
                    SQLAzureUtils.useFederation(conn, federation, memberId);

                    columns = getColumns(conn, schemaName, tableName, columnName);

                    if (columns == null || columns.length == 0) {
                        return new Column[0];
                    }
                }
                SQLAzureUtils.useRootFederation(conn);
            }
        }

        return columns;
    }

    private Column[] getColumns(
            final Connection conn,
            final DBIdentifier schemaName,
            final DBIdentifier tableName,
            final DBIdentifier columnName)
            throws SQLException {

        if (DBIdentifier.isNull(tableName) && !supportsNullTableForGetColumns) {
            return null;
        }

        String sqlSchemaName = null;
        if (!DBIdentifier.isNull(schemaName)) {
            sqlSchemaName = schemaName.getName();
        }
        if (!supportsSchemaForGetColumns) {
            sqlSchemaName = null;
        } else {
            sqlSchemaName = getSchemaNameForMetadata(schemaName);
        }

        Map.Entry<Statement, ResultSet> cols = null;

        try {
            cols = SQLAzureUtils.getColumns(
                    conn, sqlSchemaName, getTableNameForMetadata(tableName), getColumnNameForMetadata(columnName));

            final List<Column> columnList = new ArrayList<Column>();

            while (cols != null && cols.getValue() != null && cols.getValue().next()) {
                final Column column = newColumn(cols.getValue());
                columnList.add(column);

                // for opta driver, which reports nvarchar as unknown type
                String typeName = column.getTypeIdentifier().getName();

                if (typeName == null) {
                    continue;
                }

                typeName = typeName.toUpperCase();

                if ("NVARCHAR".equals(typeName)) {
                    column.setType(Types.VARCHAR);
                } else if ("UNIQUEIDENTIFIER".equals(typeName)) {
                    if (uniqueIdentifierAsVarbinary) {
                        column.setType(Types.VARBINARY);
                    } else {
                        column.setType(Types.VARCHAR);
                    }
                } else if ("NCHAR".equals(typeName)) {
                    column.setType(Types.CHAR);
                } else if ("NTEXT".equals(typeName)) {
                    column.setType(Types.CLOB);
                }
            }

            return (Column[]) columnList.toArray(new Column[columnList.size()]);
        } finally {
            if (cols != null && cols.getValue() != null) {
                try {
                    cols.getValue().close();
                    cols.getKey().close();
                } catch (Exception ignore) {
                    // ignore exception
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getCreateTableSQL(final Table table) {
        return getCreateTableSQL(table, (Federation) null);
    }

    public String[] getCreateTableSQL(final Table table, final Federation federation) {

        final List<String> toBeCreated = new ArrayList<String>();

        DataSource ds = conf.getDataSource2(null);

        if (ds != null) {
            Connection conn = null;
            try {
                conn = ds.getConnection();

                if (federation == null) {
                    toBeCreated.add(getCreateTableStm(table));
                } else {
                    // perform use federation and create table
                    toBeCreated.addAll(getStatements(table, federation));
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
     * @param federation federation.
     * @return list of statements.
     */
    private List<String> getStatements(final Table table, final Federation federation) {

        final List<String> toBeCreated = new ArrayList<String>();

        final String rangeMappingName = federation.getRangeMappingName(table.getFullIdentifier().getName());

        toBeCreated.add(
                getCreateTableStm(table)
                + (StringUtils.isBlank(rangeMappingName) ? "" : " FEDERATED ON (range_id = " + rangeMappingName + ")"));

        return toBeCreated;
    }

    /**
     * Create a standard SQL statement for table creation.
     *
     * @param table table to be created.
     * @return the value pair "table name" / "SQL statement".
     */
    private String getCreateTableStm(final Table table) {

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
        } else {
            endBuf.append(getDefaultPKConstraint(table));
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

        return buf.toString();
    }

    private String getDefaultPKConstraint(final Table table) {
        StringBuilder builder = new StringBuilder();

        for (Column column : table.getColumns()) {
            if (builder.length() > 0) {
                builder.append(",");
            }

            builder.append(column.getIdentifier().getName());
        }

        builder.insert(0, "PRIMARY KEY CLUSTERED (");
        builder.append(")");

        return builder.toString();
    }
}
