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
package net.tirasa.jpasqlazure;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Initialize {

    private static DataSource ds = null;

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
        "DROP FEDERATION FED_1",
        "DROP FEDERATION FED_2",
        "DROP FEDERATION FED_3",
        "DROP FEDERATION FED_4",
        "DROP FEDERATION FED_5"
    };

    public static void main(String args[]) {
        if (args.length != 1) {
            System.err.println("Usage java Initialization <purge|init>");
            return;
        }

        final ApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");

        ds = (DataSource) ctx.getBean("dataSource");

        if ("purge".equalsIgnoreCase(args[0])) {
            executeQueries(fedPurgeQueries);
        }

        if ("init".equalsIgnoreCase(args[0])) {
            executeQueries(fedInitQueries);
        }
    }

    public static void executeQueries(final String[] queries) {
        Connection conn = null;
        Statement stm = null;

        final ApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");

        ds = (DataSource) ctx.getBean("dataSource");

        try {
            System.out.println("Connect ...");

            conn = ds.getConnection();

            for (String query : queries) {
                System.out.println(query);

                try {
                    stm = conn.createStatement();
                    stm.execute(query);

                } catch (Exception e) {
                    System.out.println("\t\t" + e.getMessage());
                } finally {
                    if (stm != null) {
                        try {
                            stm.close();
                            stm = null;
                        } catch (SQLException ignore) {
                            //ignore
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("\t\t" + e.getMessage());
        } finally {

            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException ignore) {
                    //ignore
                }
            }
        }
    }
}
