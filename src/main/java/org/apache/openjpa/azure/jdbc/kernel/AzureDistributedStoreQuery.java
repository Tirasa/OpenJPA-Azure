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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.openjpa.azure.jdbc.AzureUniqueResultObjectProvider;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.DeleteExecutor;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.QueryExecutor;
import org.apache.openjpa.azure.jdbc.kernel.AzureSQLStoreQuery.UpdateExecutor;
import org.apache.openjpa.jdbc.kernel.AzureDistributedStoreManager;
import org.apache.openjpa.jdbc.kernel.AzureStoreManager;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.ExpressionStoreQuery;
import org.apache.openjpa.kernel.OrderingMergedResultObjectProvider;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.rop.MergedResultObjectProvider;
import org.apache.openjpa.lib.rop.RangeResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.slice.SliceThread;

/**
 * A query for distributed databases.
 *
 */
@SuppressWarnings("serial")
public class AzureDistributedStoreQuery extends AzureJDBCStoreQuery {

    private final List<StoreQuery> queries = new ArrayList<StoreQuery>();

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

    @Override
    public StoreQuery.Executor newDataStoreExecutor(final ClassMetaData meta, final boolean subs) {
        final ParallelExecutor pe = new ParallelExecutor(this, meta, subs, _parser, ctx.getCompilation());

        String tableName = null;

        if (meta != null) {
            try {
                MappingRepository repo = getStore().getConfiguration().getMappingRepositoryInstance();
                Table table = repo.getMapping(meta.getDescribedType(), meta.getEnvClassLoader(), true).getTable();
                tableName = table.getFullIdentifier().getName();
            } catch (Exception e) {
                // ignore exception and search for table by using all the connections
            }
        }

        final List<AzureStoreManager> targets = getDistributedStore().getTargets(tableName, null);

        for (AzureStoreManager target : targets) {
            final StoreQuery query = target.newQuery(language);
            query.setContext(ctx);
            queries.add(query);
        }

        return pe;
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

        public ParallelExecutor(
                final AzureDistributedStoreQuery dsq,
                final ClassMetaData meta,
                boolean subs,
                final ExpressionParser parser,
                final Object parsed) {
            super(dsq, meta, subs, parser, parsed);
            owner = dsq;
            this.meta = meta;
            this.subs = subs;
        }

        /**
         * Each child query must be executed with slice context and not the given query context.
         */
        public ResultObjectProvider executeQuery(
                final StoreQuery q, final Object[] params, final StoreQuery.Range range) {

            final List<Future<ResultObjectProvider>> futures = new ArrayList<Future<ResultObjectProvider>>();
            final List<StoreQuery.Executor> usedExecutors = new ArrayList<StoreQuery.Executor>();
            final List<ResultObjectProvider> rops = new ArrayList<ResultObjectProvider>();

            QueryContext ctx = q.getContext();

            for (StoreQuery localQuery : owner.queries) {
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

            ResultObjectProvider[] tmp = rops.toArray(new ResultObjectProvider[rops.size()]);
            ResultObjectProvider result = null;

            boolean[] ascending = getAscending(q);
            boolean isAscending = ascending.length > 0;
            boolean isAggregate = ctx.isAggregate();
            boolean hasRange = ctx.getEndRange() != Long.MAX_VALUE;

            if (isAggregate) {
                result = new AzureUniqueResultObjectProvider(tmp, q, getQueryExpressions());
            } else if (isAscending) {
                result = new OrderingMergedResultObjectProvider(
                        tmp, ascending, usedExecutors.toArray(new StoreQuery.Executor[usedExecutors.size()]), q, params);

            } else {
                result = new MergedResultObjectProvider(tmp);
            }

            if (hasRange) {
                result = new RangeResultObjectProvider(result, ctx.getStartRange(), ctx.getEndRange());
            }

            return result;
        }

        public Number executeDelete(StoreQuery q, Object[] params) {

            int result = 0;
            ExecutorService threadPool = SliceThread.getPool();

            for (StoreQuery localQuery : owner.queries) {
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

        public Number executeUpdate(StoreQuery q, Object[] params) {
            int result = 0;
            ExecutorService threadPool = SliceThread.getPool();
            for (StoreQuery localQuery : owner.queries) {
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
