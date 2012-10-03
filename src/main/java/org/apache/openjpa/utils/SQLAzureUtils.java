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
package org.apache.openjpa.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.federation.jdbc.Federation;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration.RangeType;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.springframework.security.crypto.codec.Hex;

public final class SQLAzureUtils {

    private SQLAzureUtils() {
    }

    public static void useFederation(final Connection conn, final Federation federation, final Object oid)
            throws SQLException {

        final String distribution = RangeType.UNIQUEIDENTIFIER == federation.getRangeMappingType()
                ? "'" + getUidAsString(oid) + "'" : getObjectIdAsString(oid);
        final String distributionName = federation.getDistributionName();

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("USE FEDERATION " + federation + " (" + distributionName + " = " + distribution + ") "
                + "WITH FILTERING=OFF, RESET");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignore) {
                    // ignore exception
                }
            }
        }
    }

    public static void useRootFederation(final Connection conn)
            throws SQLException {

        Statement stm = null;
        try {
            stm = conn.createStatement();
            stm.execute("USE FEDERATION ROOT WITH RESET");
        } finally {
            if (stm != null) {
                try {
                    stm.close();
                } catch (SQLException ignore) {
                    // ignore exception
                }
            }
        }
    }

    public static MemberDistribution getMemberDistribution(final Connection conn, final Federation federation)
            throws SQLException {

        final RangeType type = federation.getRangeMappingType();
        final MemberDistribution memberDistribution = new MemberDistribution(type);

        Statement stm = null;
        ResultSet federation_id = null;
        ResultSet member_distribution = null;
        try {
            stm = conn.createStatement();

            federation_id = stm.executeQuery(
                    "SELECT * "
                    + "FROM sys.Federations "
                    + "WHERE name = '" + federation.getName() + "'");

            if (federation_id.next()) {
                member_distribution = stm.executeQuery(
                        "SELECT CAST(range_high as " + type.getValue() + ") AS high "
                        + "FROM sys.federation_member_distributions "
                        + "WHERE federation_id=" + federation_id.getInt(1) + " ORDER BY high");

                while (member_distribution.next()) {
                    memberDistribution.addValue(member_distribution.getObject(1));
                }
            }

        } finally {
            if (stm != null) {
                stm.close();
            }
            if (federation_id != null) {
                federation_id.close();
            }
            if (member_distribution != null) {
                member_distribution.close();
            }
        }

        return memberDistribution;
    }

    /**
     * Check if table exist.
     *
     * @param conn given connection.
     * @param table table to be verified.
     * @param oid range id.
     * @return TRUE if exists; FALSE otherwise.
     * @throws SQLException
     */
    public static boolean tableExists(final Connection conn, final Table table)
            throws SQLException {

        boolean res = false;

        Statement stm = null;
        ResultSet rs = null;
        try {
            stm = conn.createStatement();

            rs = stm.executeQuery(
                    "SELECT OBJECT_NAME (object_id) AS[ObjectName] "
                    + "FROM sys.dm_db_partition_stats "
                    + "WHERE OBJECT_NAME(object_id) ='" + table + "'");

            if (rs.next()) {
                res = true;
            }
        } finally {
            if (rs != null) {
                rs.close();
            }

            if (stm != null) {
                stm.close();
            }
        }

        return res;
    }

    /**
     * Retrieve table columns looking for them into system tables.
     *
     * @param conn Connection.
     * @param tableName Table name.
     * @return Result set of column info.
     * @throws SQLException in case of statement failure.
     */
    public static Map.Entry<Statement, ResultSet> getColumns(
            final Connection conn, final String schemaName, final String tableName, final String columnName)
            throws SQLException {

        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ");
        queryBuilder.append("SCHEMA_NAME(o.schema_id) AS [TABLE_SCHEM], ");
        queryBuilder.append("OBJECT_NAME (c.object_id) AS [TABLE_NAME], ");
        queryBuilder.append("c.name AS [COLUMN_NAME], ");
        queryBuilder.append("c.user_type_id AS [DATA_TYPE], ");
        queryBuilder.append("TYPE_NAME (c.user_type_id) AS [TYPE_NAME], ");
        queryBuilder.append("c.max_length AS [COLUMN_SIZE], ");
        queryBuilder.append("c.precision AS [DECIMAL_DIGITS], ");
        queryBuilder.append("c.is_nullable AS [NULLABLE], ");
        queryBuilder.append("OBJECT_DEFINITION (c.default_object_id) AS [COLUMN_DEF] ");
        queryBuilder.append("FROM sys.all_columns c, sys.all_objects o ");
        queryBuilder.append("WHERE c.object_id=o.object_id ");
        queryBuilder.append("AND OBJECT_NAME (c.object_id)='").append(tableName).append("'");

        if (StringUtils.isNotBlank(schemaName)) {
            queryBuilder.append(" AND c.name='").append(columnName).append("'");
        }

        if (StringUtils.isNotBlank(columnName)) {
            queryBuilder.append(" AND SCHEMA_NAME(o.schema_id)='").append(schemaName).append("'");
        }

        final Statement stm = conn.createStatement();
        return new AbstractMap.SimpleEntry<Statement, ResultSet>(stm, stm.executeQuery(queryBuilder.toString()));
    }

    public static Set<Federation> getTargetFederation(
            final SQLAzureConfiguration conf, final Table table, Set<String> tablesToBeExcluded) {

        if (tablesToBeExcluded == null) {
            tablesToBeExcluded = new HashSet<String>();
        }

        tablesToBeExcluded.add(table.getFullIdentifier().getName());

        final Set<Federation> federations =
                new HashSet<Federation>(conf.getFederations(table.getFullIdentifier().getName()));

        final ForeignKey[] fks = table.getForeignKeys();

        for (ForeignKey fk : fks) {
            final Table extTable = fk.getPrimaryKeyTable();
            if (!tablesToBeExcluded.contains(extTable.getFullIdentifier().getName())) {
                federations.addAll(getTargetFederation(conf, extTable, tablesToBeExcluded));
            }
        }

        return federations;
    }

    private static String getObjectIdAsString(final Object oid) {
        return oid instanceof byte[] ? "0x" + new String(Hex.encode((byte[]) oid)) : oid.toString();
    }

    private static String getUidAsString(final Object oid) {
        return oid instanceof byte[] ? new String((byte[]) oid) : oid.toString();
    }
}
