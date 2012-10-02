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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.openjpa.slice.jdbc.DistributedConnection;
import org.apache.openjpa.slice.jdbc.DistributedPreparedStatement;

public class SQLAzurePreparedStatement extends DistributedPreparedStatement {

    public SQLAzurePreparedStatement(DistributedConnection c) {
        super(c);
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException {
        final SQLAzureResultSet mrs = new SQLAzureResultSet();

        for (PreparedStatement stm : this) {
            try {
                mrs.add(stm.executeQuery());
            } catch (Throwable t) {
                // ignore
            }
        }

        return mrs;
    }

    @Override
    public int executeUpdate()
            throws SQLException {

        int ret = 0;

        for (PreparedStatement stm : this) {
            ret += stm.executeUpdate();
        }
        return ret;
    }
}
