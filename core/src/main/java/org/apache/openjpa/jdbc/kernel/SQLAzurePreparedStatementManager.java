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
package org.apache.openjpa.jdbc.kernel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.federation.jdbc.FederationConfiguration;
import org.apache.openjpa.jdbc.sql.Row;
import org.apache.openjpa.jdbc.sql.RowImpl;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzurePreparedStatementManager extends BatchingPreparedStatementManagerImpl {

    public SQLAzurePreparedStatementManager(JDBCStore store, Connection conn, int batchLimit) {
        super(store, conn, batchLimit);
    }

    @Override
    protected int executeUpdate(PreparedStatement stmnt, String sql, RowImpl row)
            throws SQLException {
        // -------------------------
        // just for check configuration parameters
        // -------------------------
        _store.getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).info(
                "Retrieve federations for " + this.getClass().getSimpleName());
        final String[] federations = ((FederationConfiguration) _store.getConfiguration()).getFederationNames();
        for (String federation : federations) {
            _store.getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).info("Federation " + federation);
        }
        // -------------------------

        List<Long> range_ids = new ArrayList<Long>();

        int res = 0;

        if (row.getAction() == Row.ACTION_INSERT) {
            range_ids.add((Long) row.getVals()[row.getColumns()[0].getIndex()]);

            Statement stm = _conn.createStatement();
            stm.executeUpdate(
                    "USE FEDERATION " + SQLAzureUtils.federation + " (range_id=" + range_ids.get(0) + ") "
                    + "WITH FILTERING=OFF, RESET");
            stm.close();
        }

        return stmnt.executeUpdate();
    }
}
