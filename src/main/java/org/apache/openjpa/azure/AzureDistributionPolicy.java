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

import java.beans.PropertyDescriptor;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.slice.DistributionPolicy;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;
import org.apache.openjpa.util.ObjectId;

public class AzureDistributionPolicy implements DistributionPolicy {

    private Log log;

    @Override
    public String distribute(
            final Object pc,
            final List<String> slices,
            final Object context) {

        final Broker broker = (Broker) context;
        final AzureConfiguration conf = (AzureConfiguration) broker.getConfiguration();

        log = conf.getLog(OpenJPAConfiguration.LOG_RUNTIME);

        final Table table = AzureUtils.getTable(conf, pc.getClass());

        final List<Federation> federations = conf.getFederations(table);

        if (federations.isEmpty()) {
            return "ROOT";
        } else {
            // !!! IMPORTANT !!!
            // Every changes to this behavior must be verified against both bulk insert of objects with 
            // non auto generated id and insert of objects with auto generated id.

            final Federation fed = federations.get(0);

            Object id = null;

            try {
                final String rangeMappingName = fed.getRangeMappingName(table.getFullIdentifier().getName());

                if (StringUtils.isNotBlank(rangeMappingName)) {
                    final Object objectId = broker.getObjectId(pc);

                    if (objectId instanceof ObjectId) {
                        final Object obj = ((ObjectId) objectId).getIdObject();
                        id = new PropertyDescriptor(rangeMappingName, obj.getClass()).getReadMethod().invoke(obj);
                    } else {
                        id = new PropertyDescriptor(rangeMappingName, pc.getClass()).getReadMethod().invoke(pc);
                    }

                    if (id == null) {
                        return null;
                    }
                }
            } catch (Exception e) {
                log.error("Error retrieving objectId", e);
                return null;
            }
            
            final List<String> targets = AzureUtils.getTargetSlice(
                    (DistributedJDBCStoreManager) broker.getStoreManager().getDelegate(), slices, fed, id);

            return targets.get(0);
        }
    }
}
