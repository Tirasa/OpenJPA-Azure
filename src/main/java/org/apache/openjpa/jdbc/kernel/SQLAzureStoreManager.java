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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.openjpa.datacache.QueryCache;
import org.apache.openjpa.datacache.QueryCacheStoreQuery;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;

public class SQLAzureStoreManager extends JDBCStoreManager {

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {

        final List<Connection> connections = new ArrayList<Connection>();
        connections.add(getDataSource().getConnection());

        final SQLAzureDelegatingConnection conn = new SQLAzureDelegatingConnection(
                connections, getDataSource(), (SQLAzureConfiguration) getConfiguration());

        return new SQLAzureRefCountConnection(conn);
    }

    public class SQLAzureRefCountConnection extends RefCountConnection {

        private final Connection conn;

        public SQLAzureRefCountConnection(final Connection conn) {
            super(conn);
            this.conn = conn;
        }

        public Connection getConn() {
            return conn;
        }
    }

    public StoreQuery newQuery(String language) {
        StoreQuery sq = newStoreQuery(language);
        if (sq == null || QueryLanguages.parserForLanguage(language) == null) {
            return sq;
        }

        QueryCache queryCache = getContext().getConfiguration().getDataCacheManagerInstance().getSystemQueryCache();
        if (queryCache == null) {
            return sq;
        }

        return new QueryCacheStoreQuery(sq, queryCache);
    }

    private StoreQuery newStoreQuery(String language) {
        ExpressionParser ep = QueryLanguages.parserForLanguage(language);
        if (ep != null) {
            return new JDBCStoreQuery(this, ep);
        }
        if (QueryLanguages.LANG_SQL.equals(language)) {
            return new SQLStoreQuery(this);
        }
        if (QueryLanguages.LANG_PREPARED_SQL.equals(language)) {
            return new SQLAzurePreparedSQLStoreQuery(this);
        }
        return null;
    }
}