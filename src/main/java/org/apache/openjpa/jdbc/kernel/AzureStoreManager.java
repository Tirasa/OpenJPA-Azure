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
package org.apache.openjpa.jdbc.kernel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.kernel.AzureJDBCStoreQuery;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.datacache.QueryCache;
import org.apache.openjpa.datacache.QueryCacheStoreQuery;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;

/**
 * A specialized JDBCStoreManager for azure.
 *
 */
public class AzureStoreManager extends JDBCStoreManager {

    private final Federation fed;

    private final Object distribution;

    /**
     * Construct with immutable logical name of the slice.
     */
    public AzureStoreManager(final Federation fed, final Object distribution) {
        this.fed = fed;
        this.distribution = distribution;
    }

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {

        final Connection conn = getDataSource().getConnection();

        if (fed != null && distribution != null) {
            AzureUtils.useFederation(conn, fed, distribution);
        }

        final List<Connection> connections = new ArrayList<Connection>();
        connections.add(conn);

        final AzureDelegatingConnection azureConn = new AzureDelegatingConnection(
                connections, getDataSource(), (AzureConfiguration) getConfiguration());

        return new RefCountConnection(azureConn);
    }

    public Object getDistribution() {
        return distribution;
    }

    public Federation getFed() {
        return fed;
    }

    private StoreQuery newStoreQuery(String language) {
        ExpressionParser ep = QueryLanguages.parserForLanguage(language);
        if (ep != null) {
            return new AzureJDBCStoreQuery(this, ep);
        }
        if (QueryLanguages.LANG_SQL.equals(language)) {
            return new SQLStoreQuery(this);
        }
        if (QueryLanguages.LANG_PREPARED_SQL.equals(language)) {
            return new PreparedSQLStoreQuery(this);
        }
        return null;
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
}
