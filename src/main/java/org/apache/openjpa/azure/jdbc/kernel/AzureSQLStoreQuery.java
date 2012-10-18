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
package org.apache.openjpa.azure.jdbc.kernel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.openjpa.azure.jdbc.AzureUniqueResultObjectProvider;
import org.apache.openjpa.jdbc.kernel.AzureDistributedStoreManager;
import org.apache.openjpa.jdbc.kernel.AzureStoreManager;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.kernel.SQLStoreQuery;
import org.apache.openjpa.kernel.OrderingMergedResultObjectProvider;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.lib.rop.MergedResultObjectProvider;
import org.apache.openjpa.lib.rop.RangeResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * A query for distributed databases.
 *
 * @author Pinaki Poddar
 *
 */
@SuppressWarnings("serial")
public class AzureSQLStoreQuery extends SQLStoreQuery {

    private Map<StoreManager, StoreQuery> queries = new HashMap<StoreManager, StoreQuery>();

    private String language;

    public AzureSQLStoreQuery(final JDBCStore store, final String language) {
        super(store);
        this.language = language;
    }

    public AzureDistributedStoreManager getDistributedStore() {
        return (AzureDistributedStoreManager) getStore();
    }

    public void addQuery(final StoreManager store, final StoreQuery query) {
        queries.put(store, query);
    }

    @Override
    public void setContext(final QueryContext ctx) {
        super.setContext(ctx);
        for (StoreQuery query : queries.values()) {
            query.setContext(ctx);
        }
    }

    @Override
    public StoreQuery.Executor newDataStoreExecutor(final ClassMetaData meta, final boolean subs) {
        return new ParallelExecutor(this, meta, subs);
    }

    /**
     * Executes queries on multiple databases.
     *
     * @author Pinaki Poddar
     *
     */
    public static class ParallelExecutor extends SQLStoreQuery.SQLExecutor {

        private AzureSQLStoreQuery owner = null;

        private final ClassMetaData meta;

        private final boolean subs;

        private final List<AzureStoreManager> targets;

        public ParallelExecutor(final AzureSQLStoreQuery dsq, final ClassMetaData meta, final boolean subs) {
            super(dsq, meta);
            owner = dsq;
            this.meta = meta;
            this.subs = subs;

            targets = owner.getDistributedStore().getTargets(meta);
        }

        public ResultObjectProvider executeQuery(StoreQuery q, final Object[] params, final StoreQuery.Range range) {
            List<Future<ResultObjectProvider>> futures = new ArrayList<Future<ResultObjectProvider>>();
            final List<StoreQuery.Executor> usedExecutors = new ArrayList<StoreQuery.Executor>();
            final List<ResultObjectProvider> rops = new ArrayList<ResultObjectProvider>();

            QueryContext ctx = q.getContext();

            for (AzureStoreManager target : targets) {
                StoreQuery localQuery = owner.queries.get(target);
                Executor executor = localQuery.newDataStoreExecutor(meta, subs);

                usedExecutors.add(executor);
                QueryExecutor call = new QueryExecutor();
                call.executor = executor;
                call.query = localQuery;
                call.params = params;
                call.range = range;

                final ResultObjectProvider rop = call.call();

                if (rop != null) {
                    rops.add(rop);
                }
            }

            ResultObjectProvider result = null;

            ResultObjectProvider[] tmp = rops.toArray(new ResultObjectProvider[rops.size()]);

            boolean[] ascending = getAscending(q);
            boolean isAscending = ascending.length > 0;
            boolean isAggregate = ctx.isAggregate();
            boolean hasRange = ctx.getEndRange() != Long.MAX_VALUE;

            if (isAggregate) {
                result = new AzureUniqueResultObjectProvider(tmp, q, getQueryExpressions());
            } else if (isAscending) {
                result = new OrderingMergedResultObjectProvider(
                        tmp, ascending, usedExecutors.toArray(new StoreQuery.Executor[usedExecutors.size()]), q,
                        params);
            } else {
                result = new MergedResultObjectProvider(tmp);
            }
            if (hasRange) {
                result = new RangeResultObjectProvider(result, ctx.getStartRange(), ctx.getEndRange());
            }

            return result;
        }

        public Number executeDelete(StoreQuery q, Object[] params) {
            List<Future<Number>> futures = null;
            int result = 0;

            for (AzureStoreManager target : targets) {
                StoreQuery localQuery = owner.queries.get(target);
                Executor executor = localQuery.newDataStoreExecutor(meta, subs);
                if (futures == null) {
                    futures = new ArrayList<Future<Number>>();
                }
                DeleteExecutor call = new DeleteExecutor();
                call.executor = executor;
                call.query = localQuery;
                call.params = params;

                Number n = call.call();
                if (n != null) {
                    result += n.intValue();
                }
            }
            return result;
        }

        public Number executeUpdate(StoreQuery q, Object[] params) {
            List<Future<Number>> futures = null;
            int result = 0;

            for (AzureStoreManager target : targets) {
                StoreQuery localQuery = owner.queries.get(target);
                Executor executor = localQuery.newDataStoreExecutor(meta, subs);

                if (futures == null) {
                    futures = new ArrayList<Future<Number>>();
                }

                UpdateExecutor call = new UpdateExecutor();
                call.executor = executor;
                call.query = localQuery;
                call.params = params;

                Number n = call.call();

                if (n != null) {
                    result += n.intValue();
                }
            }
            return result;
        }
    }

    static class QueryExecutor implements Callable<ResultObjectProvider> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        StoreQuery.Range range;

        @Override
        public ResultObjectProvider call() {
            try {
                return executor.executeQuery(query, params, range);
            } catch (Throwable t) {
                // Since currently native query cannot be targeted rightly we have to catch for 
                // unexisting object exceptions. Returning 'null' we shouldn't loss too much info.
                return null;
            }
        }
    }

    static class DeleteExecutor implements Callable<Number> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        @Override
        public Number call() {
            try {
                return executor.executeDelete(query, params);
            } catch (Throwable t) {
                // Since currently native query cannot be targeted rightly we have to catch for 
                // unexisting object exceptions. Returning 'null' we shouldn't loss too much info.
                return null;
            }
        }
    }

    static class UpdateExecutor implements Callable<Number> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        @Override
        public Number call() {
            try {
                return executor.executeUpdate(query, params);
            } catch (Throwable t) {
                // Since currently native query cannot be targeted rightly we have to catch for 
                // unexisting object exceptions. Returning 'null' we shouldn't loss too much info.
                return null;
            }
        }
    }
}
