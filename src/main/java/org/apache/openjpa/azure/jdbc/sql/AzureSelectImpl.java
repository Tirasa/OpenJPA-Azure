/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */
package org.apache.openjpa.azure.jdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.AzureStoreManager.AzureRefCountConnection;
import org.apache.openjpa.jdbc.kernel.JDBCFetchConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.sql.Result;
import org.apache.openjpa.jdbc.sql.SelectImpl;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.lib.log.Log;

public class AzureSelectImpl extends SelectImpl {

    private final JDBCConfiguration _conf;

    private final Log _log;

    public AzureSelectImpl(final JDBCConfiguration conf) {
        super(conf);
        this._conf = conf;
        this._log = conf.getLog(OpenJPAConfiguration.LOG_RUNTIME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Result execute(
            final StoreContext ctx, final JDBCStore store, final JDBCFetchConfiguration fetch, final int lockLevel)
            throws SQLException {

        final Connection conn = store.getConnection();

        ((AzureDelegatingConnection) ((AzureRefCountConnection) conn).getConn()).selectWorkingConnections();

        return super.execute(ctx, store, fetch, lockLevel);
    }
}