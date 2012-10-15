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

import org.apache.openjpa.azure.jdbc.kernel.AzureDistributedStoreQuery;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.datacache.QueryCache;
import org.apache.openjpa.datacache.QueryCacheStoreQuery;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;

public class AzureDistributedStoreManager extends JDBCStoreManager {

    private final List<AzureStoreManager> stores = new ArrayList<AzureStoreManager>();

    private void initStores(final StoreContext ctx, final JDBCConfiguration conf) {
        // add the store manager for the root db ...
        AzureStoreManager store = new AzureStoreManager(null, null);
        store.setContext(ctx, conf);
        stores.add(store);

        Connection conn = null;

        try {
            final Collection<Federation> federations = ((AzureConfiguration) getConfiguration()).getFederations();

            if (!federations.isEmpty()) { // add stores for each federation member ..
                conn = getDataSource().getConnection();

                for (Federation federation : federations) {
                    for (Object memberId : AzureUtils.getMemberDistribution(conn, federation)) {
                        store = new AzureStoreManager(federation, memberId);
                        store.setContext(ctx, conf);
                        stores.add(store);
                    }
                }
            }
        } catch (SQLException e) {
            getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).fatal("Errore initializing store manager", e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ignore) {
                // ignore
            }
        }
    }

    @Override
    public void setContext(final StoreContext ctx, final JDBCConfiguration conf) {
        super.setContext(ctx, conf);
        initStores(ctx, conf);
    }

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {

        final List<Connection> connections = new ArrayList<Connection>();

        for (AzureStoreManager store : stores) {
            connections.add(store.getConnection());
        }

        final AzureDelegatingConnection conn = new AzureDelegatingConnection(
                connections, getDataSource(), (AzureConfiguration) getConfiguration());

        return new RefCountConnection(conn);
    }

    @Override
    public Collection flush(Collection sms) {
        Set<Exception> exceptions = new HashSet<Exception>();

        for (AzureStoreManager store : getTargets(null, null)) {
            store.flush(sms);
        }

        return exceptions;
    }

    private StoreQuery newStoreQuery(String language) {
        if (QueryLanguages.LANG_SQL.equals(language)) {
            return new AzureSQLStoreQuery(this, language);
        }

        final ExpressionParser parser = QueryLanguages.parserForLanguage(language);
        if (parser == null) {
            throw new UnsupportedOperationException("Language [" + language + "] not supported");
        }

        return new AzureDistributedStoreQuery(this, parser, language);
    }

    @Override
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

    public List<AzureStoreManager> getTargets(final String table, final Object distribution) {
        final List<AzureStoreManager> targets = new ArrayList<AzureStoreManager>();

        if (table == null) {
            targets.addAll(stores);
        } else {
            Connection conn = null;
            try {
                conn = getDataSource().getConnection();

                for (AzureStoreManager store : stores) {
                    final Federation fed = store.getFed();
                    final Object dist = store.getDistribution();

                    if (fed != null && fed.getTables().contains(table)) {
                        final Object memberId =
                                StringUtils.isNotBlank(fed.getDistributionName()) && distribution != null
                                ? AzureUtils.getMemberDistribution(conn, fed, distribution) : null;

                        if (memberId == null || memberId.equals(distribution)) {
                            targets.add(store);
                        }

                    }
                }
            } catch (SQLException e) {
                getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).fatal("Errore getting targets", e);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException ignore) {
                    // ignore
                }
            }

            // by default the store for root will be available
            if (targets.isEmpty()) {
                targets.add(stores.get(0));
            }
        }

        return targets;
    }
}
