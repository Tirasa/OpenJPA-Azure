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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.kernel.AzureDistributedStoreQuery;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.meta.ClassMapping;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.sql.Result;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.PCState;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.rop.MergedResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;

public class AzureDistributedStoreManager extends JDBCStoreManager {

    private final List<AzureStoreManager> stores = new ArrayList<AzureStoreManager>();

    private void initStores(final StoreContext ctx, final AzureConfiguration conf) {
        for (AzureStoreManager store : stores) {
            store.cancelAll();
            store.close();
        }

        stores.clear();

        // add the store manager for the root db ...
        AzureStoreManager store = new AzureStoreManager(null, null);
        store.setContext(ctx, conf);
        stores.add(store);

        Connection conn = null;

        try {
            final Collection<Federation> federations = conf.getFederations();

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
        initStores(ctx, (AzureConfiguration) conf);
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
    public StoreQuery newQuery(String language) {
        // In case of no federation follow the standard path
        // TODO: change this behavior by improving integration.
        if (stores.size() <= 1) {
            return super.newQuery(language);
        }

        if (QueryLanguages.LANG_SQL.equals(language)) {
            final AzureSQLStoreQuery squery = new AzureSQLStoreQuery(this, language);

            for (AzureStoreManager target : stores) {
                squery.addQuery(target, target.newQuery(language));
            }

            return squery;
        }

        final ExpressionParser parser = QueryLanguages.parserForLanguage(language);
        if (parser == null) {
            throw new UnsupportedOperationException("Language [" + language + "] not supported");
        }

        final AzureDistributedStoreQuery squery = new AzureDistributedStoreQuery(this, parser, language);
        for (AzureStoreManager target : stores) {
            squery.addQuery(target, target.newQuery(language));
        }

        return squery;
    }

    public List<AzureStoreManager> getTargets() {
        return getTargets(null);
    }

    public List<AzureStoreManager> getTargets(final ClassMetaData meta) {

        final Map.Entry<String, Object> targetId = getTargetId(meta);

        final List<AzureStoreManager> targets = new ArrayList<AzureStoreManager>();

        if (targetId.getKey() == null) {
            targets.addAll(stores);
        } else {
            Connection conn = null;
            try {
                conn = getDataSource().getConnection();

                for (AzureStoreManager store : stores) {
                    final Federation fed = store.getFed();
                    final Object dist = store.getDistribution();

                    if (fed != null && fed.getTables().contains(targetId.getKey())) {
                        final Object memberId =
                                StringUtils.isNotBlank(fed.getDistributionName()) && targetId.getValue() != null
                                ? AzureUtils.getMemberDistribution(conn, fed, targetId.getValue()) : null;

                        if (memberId == null || memberId.equals(targetId.getValue())) {
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

    private Map.Entry<String, Object> getTargetId(final ClassMetaData meta) {
        String tableName = null;

        if (meta != null) {
            try {
                final MappingRepository repo = getConfiguration().getMappingRepositoryInstance();
                final Table table = repo.getMapping(meta.getDescribedType(), meta.getEnvClassLoader(), true).getTable();
                tableName = table.getFullIdentifier().getName();
            } catch (Exception e) {
                // ignore exception and search for table by using all the connections
            }
        }

        // TODO: improve looking for distribution value.
        return new AbstractMap.SimpleEntry<String, Object>(tableName, null);
    }

    private void collectException(Collection error, Collection holder) {
        if (!(error == null || error.isEmpty())) {
            holder.addAll(error);
        }
    }

    @Override
    public Collection flush(Collection sms) {
        // TODO: take care of consider multi-threading (look at DistributedJDBCStoreManager)

        final Set<Exception> exceptions = new HashSet<Exception>();

        for (AzureStoreManager store : stores) {
            store.flush(sms);
        }

        super.flush(sms);

        // TODO: we could improve this behavior with the following.
        // Actually join tables are ignored by the following code but we have to fix this issue asap.
//        for (Map.Entry<AzureStoreManager, Set<OpenJPAStateManager>> targetedSMS : getTargetedSMS(sms).entrySet()) {
//            if (!targetedSMS.getValue().isEmpty()) {
////                collectException(targetedSMS.getKey().flush(targetedSMS.getValue()), exceptions);
//                targetedSMS.getKey().flush(targetedSMS.getValue());
//            }
//        }

        // TODO: ignore exception! currently we are not distiguishing between target members.
        return exceptions;
    }

    @Override
    public boolean cancelAll() {
        boolean ret = true;

        for (AzureStoreManager store : stores) {
            ret &= store.cancelAll();
        }

        ret &= super.cancelAll();

        return ret;
    }

    @Override
    public void beginOptimistic() {
        for (AzureStoreManager store : stores) {
            store.beginOptimistic();
        }

        super.beginOptimistic();
    }

    @Override
    public ResultObjectProvider executeExtent(
            final ClassMetaData meta, final boolean subclasses, final FetchConfiguration fetch) {

        final List<AzureStoreManager> targets = getTargets(meta);
        final List<ResultObjectProvider> rops = new ArrayList<ResultObjectProvider>(targets.size());

        for (AzureStoreManager store : targets) {
            rops.add(store.executeExtent(meta, subclasses, fetch));
        }

        return new MergedResultObjectProvider(rops.toArray(new ResultObjectProvider[targets.size()]));
    }

    @Override
    public boolean exists(final OpenJPAStateManager sm, final Object context) {
        boolean ret = false;

        for (AzureStoreManager store : stores) {
            ret |= store.exists(sm, context);
        }

        ret |= super.exists(sm, context);

        return ret;
    }

    @Override
    public Object load(
            final ClassMapping mapping, final JDBCFetchConfiguration fetch, final BitSet exclude, final Result result)
            throws SQLException {
        return getTargets(mapping).get(0).load(mapping, fetch, exclude, result);
    }

    @Override
    public boolean load(OpenJPAStateManager sm, BitSet fields, FetchConfiguration fetch, int lockLevel, Object context) {
        return getTargets(sm.getMetaData()).get(0).load(sm, fields, fetch, lockLevel, context);
    }

    @Override
    public Collection loadAll(
            final Collection sms, final PCState state, final int load, FetchConfiguration fetch, final Object edata) {

        final Collection result = new ArrayList();

        for (Map.Entry<AzureStoreManager, Set<OpenJPAStateManager>> targetedSMS : getTargetedSMS(sms).entrySet()) {

            if (!targetedSMS.getValue().isEmpty()) {

                final Collection tmp = targetedSMS.getKey().loadAll(targetedSMS.getValue(), state, load, fetch, edata);

                if (tmp != null && !tmp.isEmpty()) {
                    result.addAll(tmp);
                }
            }
        }

        return result;
    }

    private Map<AzureStoreManager, Set<OpenJPAStateManager>> getTargetedSMS(final Collection sms) {
        final Map<AzureStoreManager, Set<OpenJPAStateManager>> subsets =
                new HashMap<AzureStoreManager, Set<OpenJPAStateManager>>();

        for (Object sm : sms) {
            for (AzureStoreManager target : getTargets(((OpenJPAStateManager) sm).getMetaData())) {
                Set<OpenJPAStateManager> subset = subsets.get(target);

                if (subset == null) {
                    subset = new HashSet<OpenJPAStateManager>();
                    subsets.put(target, subset);
                }

                subset.add((OpenJPAStateManager) sm);
            }
        }

        return subsets;
    }
}
