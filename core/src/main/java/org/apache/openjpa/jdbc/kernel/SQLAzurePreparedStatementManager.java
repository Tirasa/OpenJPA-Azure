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
import org.apache.openjpa.federation.jdbc.FederationConfiguration;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.sql.Row;
import org.apache.openjpa.jdbc.sql.RowImpl;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzurePreparedStatementManager extends BatchingPreparedStatementManagerImpl {

    public SQLAzurePreparedStatementManager(final JDBCStore store, final Connection conn, final int batchLimit) {
        super(store, conn, batchLimit);
    }

    @Override
    protected int executeUpdate(final PreparedStatement stmnt, final String sql, final RowImpl row)
            throws SQLException {

        if (row.getAction() == Row.ACTION_INSERT) {
            final String rangeMappingName = ((FederationConfiguration) _store.getConfiguration()).getRangeMappingName();

            final Column col = row.getTable().getColumn(
                    DBIdentifier.newIdentifier(rangeMappingName, DBIdentifier.DBIdentifierType.COLUMN, true), false);
            
            SQLAzureUtils.useFederation(_conn, row.getVals()[col.getIndex()]);
        }

        return stmnt.executeUpdate();
    }
}
