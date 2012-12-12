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
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.datacache.DataCacheStoreManager;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.slice.ReplicationPolicy;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;

public class AzureReplicationPolicy implements ReplicationPolicy {

    @Override
    public String[] replicate(final Object pc, final List<String> slices, final Object context) {

        final Broker broker = (Broker) context;
        final AzureConfiguration conf = (AzureConfiguration) broker.getConfiguration();

        final Table table = AzureUtils.getTable(conf, pc.getClass());

        final List<Federation> federations = conf.getFederations(table);

        if (federations.isEmpty()) {
            return new String[]{"ROOT"};
        } else {

            final List<String> rep = new ArrayList<String>(federations.size());

            final Object objectId = broker.getObjectId(pc);

            final StoreManager store = broker.getStoreManager().getDelegate() instanceof DataCacheStoreManager
                    ? ((DataCacheStoreManager) broker.getStoreManager().getDelegate()).getDelegate()
                    : broker.getStoreManager().getDelegate();

            for (Federation fed : federations) {
                final String rangeMappingName = fed.getRangeMappingName(table.getFullIdentifier().getName());
                final Object id = objectId == null ? null : AzureUtils.getObjectIdValue(objectId, rangeMappingName);

                final List<String> targets =
                        AzureUtils.getTargetSlice((DistributedJDBCStoreManager) store, slices, fed, id);

                if (targets.isEmpty() || StringUtils.isBlank(rangeMappingName)) {
                    rep.addAll(targets);
                } else {
                    rep.add(targets.get(0));
                }
            }

            return rep.toArray(new String[federations.size()]);
        }
    }
}
