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

import java.lang.Object;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.kernel.AzureJDBCStoreQuery;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.datacache.QueryCache;
import org.apache.openjpa.datacache.QueryCacheStoreQuery;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.PreparedSQLStoreQuery;
import org.apache.openjpa.jdbc.kernel.SQLStoreQuery;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.PCState;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.slice.Slice;
import org.apache.openjpa.slice.jdbc.SliceStoreManager;

public class AzureSliceStoreManager extends SliceStoreManager {

    public static Map<Federation, List<Object>> federations = null;

    private Object fedUpperBound = null;

    private boolean fedMultiMember = false;

    private Federation federation = null;

    private AzureConfiguration azureConf;

    private static Log log = null;

    public AzureSliceStoreManager(Slice slice) {
        super(slice);
    }

    @Override
    public void setContext(StoreContext ctx, JDBCConfiguration conf) {
        super.setContext(ctx, conf);
        azureConf = (AzureConfiguration) ctx.getConfiguration();

        if (log == null) {
            log = conf.getLog(JDBCConfiguration.LOG_DIAG);
        }

        if (AzureSliceStoreManager.federations == null || AzureSliceStoreManager.federations.isEmpty()) {
            Connection conn = null;
            try {
                conn = getNewConnection();
                initFederations(azureConf, conn);
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

        if (federation != null) {
            initMembers();
        }
    }

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {
        final RefCountConnection conn = super.connectInternal();

        if (federation != null) {
            AzureUtils.useFederation(conn, federation, fedUpperBound);
        }

        return conn;
    }

    public void setFederation(final Federation federation) {
        this.federation = federation;
    }

    private void initMembers() {
        final List<Object> members = federations.get(federation);
        final int memberIndexPos = AzureUtils.getSliceMemberIndex(getName());
        fedMultiMember = members.size() > 1;
        fedUpperBound = members.get(memberIndexPos);
    }

    public Object getFedUpperBound() {
        return fedUpperBound;
    }

    public boolean isFedMultiMember() {
        return fedMultiMember;
    }

    public String getFedName() {
        return federation == null ? null : federation.getName();
    }

    public Federation getFederation() {
        return federation;
    }

    // ---------------------------------
    // Just for min implemenation
    // ---------------------------------
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
    // ---------------------------------

    public static void initFederations(final AzureConfiguration conf, final Connection conn) {
        if (log == null) {
            log = conf.getLog(JDBCConfiguration.LOG_DIAG);
        }

        Collection<Federation> feds = conf.getFederations();
        AzureSliceStoreManager.federations = new HashMap<Federation, List<Object>>(feds.size());

        for (Federation fed : feds) {

            List<Object> members = federations.get(fed);

            if (members == null) {
                members = new ArrayList<Object>();
                federations.put(fed, members);
            }

            try {
                for (Object obj : AzureUtils.getMemberDistribution(conn, fed)) {
                    log.info("Init member '" + obj + "' for " + fed);
                    members.add(obj);
                }
            } catch (SQLException e) {
                log.error("Error searching for federation members", e);
            }

        }
    }
}
