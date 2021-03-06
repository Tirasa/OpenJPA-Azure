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
package org.apache.openjpa.azure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.datacache.DataCacheStoreManager;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.slice.FinderTargetPolicy;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;

/**
 *
 * @author fabio
 */
public class AzureFinderTargetPolicy implements FinderTargetPolicy {

    @Override
    public String[] getTargets(
            final Class<?> cls,
            final Object oid,
            final List<String> slices,
            final Object context) {

        final Broker broker = (Broker) context;
        final AzureConfiguration conf = (AzureConfiguration) broker.getConfiguration();

        Log log = conf.getLog(JDBCConfiguration.LOG_DIAG);

        log.info("Evaluate target policy for '" + cls.getSimpleName() + ":" + oid + "'");

        final Table table = AzureUtils.getTable(conf, cls);

        log.info("Search location for table " + table);

        final Set<Federation> federations = new HashSet<Federation>(conf.getFederations(table));

        final List<String> result = new ArrayList<String>();

        if (federations.isEmpty()) {
            result.add("ROOT");
        } else {
            final StoreManager store = broker.getStoreManager().getDelegate() instanceof DataCacheStoreManager
                    ? ((DataCacheStoreManager) broker.getStoreManager().getDelegate()).getDelegate()
                    : broker.getStoreManager().getDelegate();

            for (Federation federation : federations) {
                final Object id = AzureUtils.getObjectIdValue(
                        oid, federation.getRangeMappingName(table.getFullIdentifier().getName()));

                result.addAll(AzureUtils.getTargetSlice((DistributedJDBCStoreManager) store, slices, federation, id));
            }

        }

        log.info("Retrieved targets " + result);

        return result.toArray(new String[result.size()]);
    }
}
