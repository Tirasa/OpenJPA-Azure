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
import java.util.ArrayList;
import java.util.List;
import org.apache.openjpa.jdbc.schema.Table;

public class SQLAzureUtils {

    public static String federation = "FED_1";

    public static List<Long> getMemberDistribution(
            final Connection conn)
            throws SQLException {

        final List<Long> range_ids = new ArrayList<Long>();

        Statement stm = null;
        ResultSet federation_id = null;
        ResultSet member_distribution = null;

        try {
            stm = conn.createStatement();

            federation_id = stm.executeQuery(
                    "SELECT * "
                    + "FROM sys.Federations "
                    + "WHERE name = '" + federation + "'");

            if (federation_id.next()) {
                member_distribution = stm.executeQuery(
                        "SELECT CAST(range_high as bigint) AS high "
                        + "FROM sys.federation_member_distributions "
                        + "WHERE federation_id=" + federation_id.getInt(1) + " ORDER BY high");

                while (member_distribution.next()) {
                    range_ids.add(member_distribution.getLong(1));
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

        return range_ids;
    }

    /**
     * Check if table exist.
     *
     * @param conn given connection.
     * @param table table to be verified.
     * @param id range id.
     * @return TRUE if exists; FALSE otherwise.
     * @throws SQLException
     */
    public static boolean tableExists(
            Connection conn, Table table, Long id)
            throws SQLException {
        boolean res = false;

        Statement stm = null;
        ResultSet rs = null;

        try {
            stm = conn.createStatement();

            stm.execute("USE FEDERATION " + federation + " (range_id=" + id + ") WITH FILTERING = OFF, RESET");

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
}
