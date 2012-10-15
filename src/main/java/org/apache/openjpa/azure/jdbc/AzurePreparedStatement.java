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
package org.apache.openjpa.azure.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.openjpa.slice.jdbc.DistributedPreparedStatement;

public class AzurePreparedStatement extends DistributedPreparedStatement {

    private final int workingIndex;

    public AzurePreparedStatement(final AzureDelegatingConnection conn) {

        super(conn);
        workingIndex = conn.getWorkingIndex();
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException {

        final AzureResultSet mrs = new AzureResultSet();

        for (PreparedStatement stm : this) {
            try {
                mrs.add(stm.executeQuery());
            } catch (Throwable t) {
                // TODO: ignore since joins have been not supported yet
            }
        }

        return mrs;
    }

    @Override
    public int executeUpdate()
            throws SQLException {

        SQLException exception = null;

        boolean found = false;

        int ret = 0;

        for (PreparedStatement stm : this) {
            try {
                ret += stm.executeUpdate();
                found = true;
            } catch (Throwable t) {
                // TODO: current implementation is needed to support native query
                if (exception == null) {
                    exception = new SQLException(t);
                } else {
                    exception.setNextException(new SQLException(t));
                }
            }
        }

        if (!found && exception != null) {
            throw exception;
        }

        // TODO: keep under control! This behavior have to be verified step-by-step.
        if (workingIndex == 0 || ret != workingIndex) {
            // update or delete of a specific object
            return ret;
        } else {
            // probably an insert or an update of the same object among different federations/members.
            return 1;
        }
    }
}
