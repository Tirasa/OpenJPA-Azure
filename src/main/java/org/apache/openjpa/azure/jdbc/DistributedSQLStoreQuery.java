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
package org.apache.openjpa.azure.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.azure.util.NativeQueryInfo;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;

import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.kernel.SQLStoreQuery;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.OrderingMergedResultObjectProvider;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.lib.jdbc.ReportingSQLException;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.lib.rop.MergedResultObjectProvider;
import org.apache.openjpa.lib.rop.RangeResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.slice.DistributedConfiguration;
import org.apache.openjpa.slice.SliceThread;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;
import org.apache.openjpa.slice.jdbc.SliceStoreManager;
import org.apache.openjpa.util.StoreException;

/**
 * A query for distributed databases.
 *
 * @author Pinaki Poddar
 *
 */
@SuppressWarnings("serial")
public class DistributedSQLStoreQuery extends SQLStoreQuery {

    private List<StoreQuery> _queries = new ArrayList<StoreQuery>();

    private final Log log;

    public DistributedSQLStoreQuery(JDBCStore store) {
        super(store);
        log = store.getConfiguration().getLog(JDBCConfiguration.LOG_DIAG);
    }

    public void add(StoreQuery q) {
        _queries.add(q);
    }

    public DistributedJDBCStoreManager getDistributedStore() {
        return (DistributedJDBCStoreManager) getStore();
    }

    @Override
    public StoreQuery.Executor newDataStoreExecutor(ClassMetaData meta, boolean subs) {
        boolean parallel = !getContext().getStoreContext().getBroker().getMultithreaded();
        DistributedSQLStoreQuery.ParallelExecutor ex = new DistributedSQLStoreQuery.ParallelExecutor(this, meta,
                parallel);
        for (StoreQuery q : _queries) {
            ex.addExecutor(q.newDataStoreExecutor(meta, subs));
        }
        return ex;
    }

    public void setContext(QueryContext ctx) {
        super.setContext(ctx);
        for (StoreQuery q : _queries) {
            q.setContext(ctx);
        }
    }

    /**
     * Executes queries on multiple databases.
     *
     * @author Pinaki Poddar
     *
     */
    public static class ParallelExecutor extends SQLStoreQuery.SQLExecutor {

        private List<StoreQuery.Executor> executors = new ArrayList<StoreQuery.Executor>();

        private DistributedSQLStoreQuery owner = null;

        public ParallelExecutor(DistributedSQLStoreQuery dsq, ClassMetaData meta, boolean p) {
            super(dsq, meta);
            owner = dsq;
        }

        public void addExecutor(StoreQuery.Executor ex) {
            executors.add(ex);
        }

        /**
         * Each child query must be executed with slice context and not the given query context.
         */
        public ResultObjectProvider executeQuery(StoreQuery q, final Object[] params, final StoreQuery.Range range) {

            final List<Future<ResultObjectProvider>> futures = new ArrayList<Future<ResultObjectProvider>>();
            final List<StoreQuery.Executor> usedExecutors = new ArrayList<StoreQuery.Executor>();
            final List<ResultObjectProvider> rops = new ArrayList<ResultObjectProvider>();

            final List<SliceStoreManager> targets = findTargets();

            final QueryContext ctx = q.getContext();
            final boolean isReplicated = containsReplicated(ctx);

            Federation previousFed = null;

            final ExecutorService threadPool = SliceThread.getPool();

            for (int i = 0; i < owner._queries.size(); i++) {
                StoreManager sm = owner.getDistributedStore().getSlice(i);

                final Federation fed = ((AzureSliceStoreManager) sm).getFederation();

                if (previousFed != null) {
                    // ------------------------------------------
                    // Check if replicated among different federations
                    // ------------------------------------------
                    // if replicated, then execute only on single slice
                    if (previousFed != null && !previousFed.equals(fed) && isReplicated) {
                        break;
                    }
                    // ------------------------------------------

                    // ------------------------------------------
                    // Check if object is "locally" replicated (among members of the same federation);
                    // * get table name and federation;
                    // * get rangeMappingName;
                    // * if rangeMappingName is null then the object is "locally" replicated.
                    // ------------------------------------------
                    if (previousFed != null && previousFed.equals(fed)
                            && DistributedSQLStoreQuery.ParallelExecutor.isLocallyReplicated(ctx, fed)) {
                        break;
                    }
                    // ------------------------------------------
                }

                if (!targets.contains(sm)) {
                    continue;
                }

                previousFed = fed;

                StoreQuery query = owner._queries.get(i);
                StoreQuery.Executor executor = executors.get(i);

                usedExecutors.add(executor);
                DistributedSQLStoreQuery.QueryExecutor call = new DistributedSQLStoreQuery.QueryExecutor();
                call.executor = executor;
                call.query = query;
                call.params = params;
                call.range = range;

                owner.log.info("[" + ((AzureSliceStoreManager) sm).getSlice().getName() + "] Execute query: "
                        + query.getContext().getQueryString());

                futures.add(threadPool.submit(call));
            }

            for (Future<ResultObjectProvider> future : futures) {
                try {
                    rops.add(future.get());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new StoreException(e.getCause());
                }
            }

            ResultObjectProvider[] arops = rops.toArray(new ResultObjectProvider[rops.size()]);
            ResultObjectProvider result = null;
            boolean[] ascending = getAscending(q);
            boolean isAscending = ascending.length > 0;

            // TODO: remove temporary patch for aggregate function COUNT
            boolean isAggregate = ctx.isAggregate();
            boolean isNativeAggregate = ctx.getQueryString().matches(".*COUNT(.*).*");
            
            boolean hasRange = ctx.getEndRange() != Long.MAX_VALUE;

            if (isAggregate) {
                result = new AzureUniqueResultObjectProvider(arops, q, getQueryExpressions());
            }if (isNativeAggregate) {
                result = new AzureNativeAggregatorROP(arops, q, getQueryExpressions());
            } else if (isAscending) {
                result = new OrderingMergedResultObjectProvider(
                        arops,
                        ascending,
                        usedExecutors.toArray(new StoreQuery.Executor[usedExecutors.size()]), q, params);
            } else {
                result = new MergedResultObjectProvider(arops);
            }
            if (hasRange) {
                result = new RangeResultObjectProvider(result, ctx.getStartRange(), ctx.getEndRange());
            }
            return result;
        }

        static boolean isLocallyReplicated(final QueryContext query, final Federation fed) {
            final AzureConfiguration conf = (AzureConfiguration) query.getStoreContext().getConfiguration();

            final List<String> tables = new ArrayList<String>();

            final Class<?> candidate = query.getCandidateType();
            if (candidate == null) {
                final ClassMetaData[] metas = query.getAccessPathMetaDatas();
                if (metas == null || metas.length < 1) {
                    tables.addAll(new NativeQueryInfo(query.getQueryString()).getTableNames());
                } else {
                    for (ClassMetaData meta : metas) {
                        tables.add(AzureUtils.getTable(conf, meta).getFullIdentifier().getName());
                    }
                }
            } else {
                tables.add(AzureUtils.getTable(conf, candidate).getFullIdentifier().getName());
            }

            boolean res = true;
            boolean auto = false;

            for (String tableName : tables) {

                // there is at least one explicitely federated object
                if (fed.getTables().contains(tableName)) {
                    auto = true;
                }

                // there is at least one explicitely federated object not locally federated
                if (fed.getTables().contains(tableName) && fed.getRangeMappingName(tableName) != null) {
                    res = false;
                }
            }


            return auto && res;
        }

        /**
         * Scans metadata to find out if a replicated class is the candidate.
         */
        boolean containsReplicated(QueryContext query) {
            final Class<?> candidate = query.getCandidateType();
            final DistributedConfiguration conf = (DistributedConfiguration) query.getStoreContext().getConfiguration();

            if (candidate != null) {
                return conf.isReplicated(candidate);
            }

            ClassMetaData[] metas = query.getAccessPathMetaDatas();

            if (metas == null || metas.length < 1) {
                return false;
            }

            for (ClassMetaData meta : metas) {
                if (conf.isReplicated(meta.getDescribedType())) {
                    return true;
                }
            }

            return false;
        }

        public Number executeDelete(StoreQuery q, Object[] params) {
            List<Future<Number>> futures = null;
            int result = 0;

            List<SliceStoreManager> targets = findTargets();

            ExecutorService threadPool = SliceThread.getPool();

            for (int i = 0; i < owner._queries.size(); i++) {
                StoreManager sm = owner.getDistributedStore().getSlice(i);
                if (!targets.contains(sm)) {
                    continue;
                }
                StoreQuery query = owner._queries.get(i);
                StoreQuery.Executor executor = executors.get(i);

                if (futures == null) {
                    futures = new ArrayList<Future<Number>>();
                }

                DistributedSQLStoreQuery.DeleteExecutor call = new DistributedSQLStoreQuery.DeleteExecutor();
                call.executor = executor;
                call.query = query;
                call.params = params;

                owner.log.info("[" + ((AzureSliceStoreManager) sm).getSlice().getName() + "] Execute delete query: "
                        + query.getContext().getQueryString());

                futures.add(threadPool.submit(call));
            }
            for (Future<Number> future : futures) {
                try {
                    Number n = future.get();
                    if (n != null) {
                        result += n.intValue();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new StoreException(e.getCause());
                }
            }
            return result;
        }

        public Number executeUpdate(StoreQuery q, Object[] params) {
            List<Future<Number>> futures = null;
            int result = 0;

            List<SliceStoreManager> targets = findTargets();

            ExecutorService threadPool = SliceThread.getPool();
            for (int i = 0; i < owner._queries.size(); i++) {
                StoreManager sm = owner.getDistributedStore().getSlice(i);

                if (!targets.contains(sm)) {
                    continue;
                }

                StoreQuery query = owner._queries.get(i);
                StoreQuery.Executor executor = executors.get(i);

                if (futures == null) {
                    futures = new ArrayList<Future<Number>>();
                }

                DistributedSQLStoreQuery.UpdateExecutor call = new DistributedSQLStoreQuery.UpdateExecutor();
                call.executor = executor;
                call.query = query;
                call.params = params;

                owner.log.info("[" + ((AzureSliceStoreManager) sm).getSlice().getName() + "] Execute update query: "
                        + query.getContext().getQueryString());

                futures.add(threadPool.submit(call));
            }

            for (Future<Number> future : futures) {
                try {
                    Number n = future.get();
                    result += (n == null) ? 0 : n.intValue();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    final Throwable ex = e.getCause();
                    int code = ex != null && ex.getCause() instanceof ReportingSQLException
                            ? ((ReportingSQLException) ex.getCause()).getErrorCode()
                            : 0;

                    // In case of native inserts we cannot avoid SQLExceptions retrieved inserting rows in wrong members
                    if (code != 45022) {
                        throw new StoreException(e.getCause());
                    }
                }
            }

            return result;
        }

        List<SliceStoreManager> findTargets() {
            FetchConfiguration fetch = owner.getContext().getFetchConfiguration();
            return owner.getDistributedStore().getTargets(fetch);
        }
    }

    static class QueryExecutor implements Callable<ResultObjectProvider> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        StoreQuery.Range range;

        public ResultObjectProvider call()
                throws Exception {
            return executor.executeQuery(query, params, range);
        }
    }

    static class DeleteExecutor implements Callable<Number> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        public Number call()
                throws Exception {
            return executor.executeDelete(query, params);
        }
    }

    static class UpdateExecutor implements Callable<Number> {

        StoreQuery query;

        StoreQuery.Executor executor;

        Object[] params;

        public Number call()
                throws Exception {
            return executor.executeUpdate(query, params);
        }
    }
}
