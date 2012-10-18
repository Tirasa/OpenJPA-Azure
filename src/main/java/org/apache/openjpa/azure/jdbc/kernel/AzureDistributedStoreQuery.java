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
import org.apache.openjpa.azure.jdbc.AzureUniqueResultObjectProvider;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.DeleteExecutor;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.QueryExecutor;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.UpdateExecutor;
import org.apache.openjpa.jdbc.kernel.AzureDistributedStoreManager;
import org.apache.openjpa.jdbc.kernel.AzureStoreManager;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.kernel.ExpressionStoreQuery;
import org.apache.openjpa.kernel.OrderingMergedResultObjectProvider;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.rop.MergedResultObjectProvider;
import org.apache.openjpa.lib.rop.RangeResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * A query for distributed databases.
 *
 */
@SuppressWarnings("serial")
public class AzureDistributedStoreQuery extends AzureJDBCStoreQuery {

    private Map<StoreManager, StoreQuery> queries = new HashMap<StoreManager, StoreQuery>();

    private ExpressionParser _parser;

    private final String language;

    public AzureDistributedStoreQuery(final JDBCStore store, final ExpressionParser parser, final String language) {
        super(store, parser);
        _parser = parser;
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
        return new ParallelExecutor(this, meta, subs, _parser, ctx.getCompilation());
    }

    /**
     * Executes queries on multiple databases.
     *
     * @author Pinaki Poddar
     *
     */
    public static class ParallelExecutor extends ExpressionStoreQuery.DataStoreExecutor {

        private AzureDistributedStoreQuery owner = null;

        private final ClassMetaData meta;

        private final boolean subs;

        private final List<AzureStoreManager> targets;

        public ParallelExecutor(
                final AzureDistributedStoreQuery dsq,
                final ClassMetaData meta,
                final boolean subs,
                final ExpressionParser parser,
                final Object parsed) {
            super(dsq, meta, subs, parser, parsed);
            owner = dsq;
            this.meta = meta;
            this.subs = subs;

            targets = owner.getDistributedStore().getTargets(meta);
        }

        @Override
        public ResultObjectProvider executeQuery(
                final StoreQuery query, final Object[] params, final StoreQuery.Range range) {

            final List<StoreQuery.Executor> usedExecutors = new ArrayList<StoreQuery.Executor>();
            final List<ResultObjectProvider> rops = new ArrayList<ResultObjectProvider>();

            QueryContext ctx = query.getContext();

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

            boolean[] ascending = getAscending(query);
            boolean isAscending = ascending.length > 0;
            boolean isAggregate = ctx.isAggregate();
            boolean hasRange = ctx.getEndRange() != Long.MAX_VALUE;

            if (isAggregate) {
                result = new AzureUniqueResultObjectProvider(tmp, query, getQueryExpressions());
            } else if (isAscending) {
                result = new OrderingMergedResultObjectProvider(
                        tmp,
                        ascending,
                        usedExecutors.toArray(new StoreQuery.Executor[usedExecutors.size()]),
                        query,
                        params);

            } else {
                result = new MergedResultObjectProvider(tmp);
            }

            if (hasRange) {
                result = new RangeResultObjectProvider(result, ctx.getStartRange(), ctx.getEndRange());
            }

            return result;
        }

        @Override
        public Number executeDelete(StoreQuery q, Object[] params) {

            int result = 0;

            for (AzureStoreManager target : targets) {
                StoreQuery localQuery = owner.queries.get(target);
                Executor executor = localQuery.newDataStoreExecutor(meta, subs);
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

        @Override
        public Number executeUpdate(StoreQuery q, Object[] params) {
            int result = 0;

            for (AzureStoreManager target : targets) {
                StoreQuery localQuery = owner.queries.get(target);
                Executor executor = localQuery.newDataStoreExecutor(meta, subs);
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
}
