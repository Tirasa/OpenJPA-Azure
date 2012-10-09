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
package org.apache.openjpa.azure.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

public final class Initialize {

    private static String[] fedInitQueries = {
        "USE FEDERATION ROOT WITH RESET",
        "CREATE FEDERATION FED_1 (range_id BIGINT RANGE)",
        "ALTER FEDERATION FED_1 SPLIT AT (range_id=5)",
        "CREATE FEDERATION FED_2 (range_id UNIQUEIDENTIFIER RANGE)",
        "ALTER FEDERATION FED_2 SPLIT AT (range_id='00000000-0000-0000-0000-000000000005')",
        "CREATE FEDERATION FED_3 (range_id int RANGE)",
        "ALTER FEDERATION FED_3 SPLIT AT (range_id=5)",
        "CREATE FEDERATION FED_4 (range_id VARBINARY(100) RANGE)",
        "CREATE FEDERATION FED_5 (range_id BIGINT RANGE)"
    };

    private static String[] fedPurgeQueries = {
        "USE FEDERATION ROOT WITH RESET",
        "DROP TABLE Membership",
        "DROP TABLE OPENJPA_SEQUENCE_TABLE",
        "DROP FEDERATION FED_1",
        "DROP FEDERATION FED_2",
        "DROP FEDERATION FED_3",
        "DROP FEDERATION FED_4",
        "DROP FEDERATION FED_5"
    };

    private static Connection conn = null;

    private Initialize() {
    }

    public static void main(final String args[]) {
        if (args.length != 1) {
            System.err.println("Usage java Initialization <purge|init>");
            return;
        }

        final EntityManagerFactory emf = Persistence.createEntityManagerFactory("azure-test");
        final Map<String, Object> properties = emf.getProperties();

        final String url = properties.get("openjpa.ConnectionURL").toString();
        final String driver = properties.get("openjpa.ConnectionDriverName").toString();
        final String uid = properties.get("openjpa.ConnectionUserName").toString();
        final String pwd = properties.get("openjpa.ConnectionPassword").toString();

        System.out.println(" * URL: " + url);
        System.out.println(" * Driver name: " + driver);
        System.out.println(" * Username: " + uid);
        System.out.println(" * Password: " + pwd);

        try {
            // Load the Driver class.
            Class.forName(driver);

            //Create the connection using the static getConnection method
            conn = DriverManager.getConnection(url, uid, pwd);

            if ("purge".equalsIgnoreCase(args[0])) {
                executeQueries(fedPurgeQueries);
            }

            if ("init".equalsIgnoreCase(args[0])) {
                executeQueries(fedInitQueries);
            }

        } catch (Throwable t) {
            t.printStackTrace();
            System.err.println("\t\t" + t.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignore) {
                    // ignore
                }
            }
        }
    }

    public static void executeQueries(final String[] queries) {
        for (String query : queries) {
            System.out.println(query);

            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                stmt.execute(query);
            } catch (Exception e) {
                System.out.println("\t\t" + e.getMessage());
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                        stmt = null;
                    } catch (SQLException ignore) {
                        //ignore
                    }
                }
            }
        }
    }
}
