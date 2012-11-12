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

import java.util.List;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.slice.DistributionPolicy;

public class AzureDistributionPolicy implements DistributionPolicy {

    @Override
    public String distribute(
            final Object pc,
            final List<String> slices,
            final Object context) {

        final Broker broker = (Broker) context;
        final AzureConfiguration conf = (AzureConfiguration) broker.getConfiguration();

        final Table table = AzureUtils.getTable(conf, pc.getClass());

        List<Federation> federations = conf.getFederations(table);
        if (federations.isEmpty()) {
            return "ROOT";
        } else {
            return federations.get(0).getName();
        }
    }
}
