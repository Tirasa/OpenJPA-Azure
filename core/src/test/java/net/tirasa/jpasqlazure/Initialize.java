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

public class PurgeAll {

    private static String[] queries = {
        "drop table OPENJPA_SEQUENCE_TABLE",
        "drop table BusinessRole_Person",
        "drop table BusinessRole",
        "USE FEDERATION FED_1 (range_id=0) WITH FILTERING=OFF, RESET",
        "drop table Person",
        "USE FEDERATION FED_1 (range_id=5) WITH FILTERING=OFF, RESET",
        "drop table Person",
        "USE FEDERATION FED_2 (range_id = '00000000-0000-0000-0000-000000000000') WITH RESET, FILTERING = OFF",
        "drop table PersonUID",
        "USE FEDERATION FED_3 (range_id = 0) WITH RESET, FILTERING = OFF",
        "drop table PersonINT",
        "USE FEDERATION FED_3 (range_id = 5) WITH RESET, FILTERING = OFF",
        "drop table PersonINT",
        "USE FEDERATION FED_4 (range_id = 0) WITH RESET, FILTERING = OFF",
        "drop table PersonBIN"
    };

    public static void main(String args[]) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");
        final DataSource dataSource = (DataSource) ctx.getBean("dataSource");

        Connection conn = null;
        Statement stm = null;

        try {
            System.out.println("Connect ...");

            conn = dataSource.getConnection();

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
