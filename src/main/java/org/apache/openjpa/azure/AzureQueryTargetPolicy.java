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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.meta.AzureMappingTool;
import org.apache.openjpa.azure.util.NativeQueryInfo;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.identifier.QualifiedDBIdentifier;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.meta.MappingTool;
import org.apache.openjpa.jdbc.schema.SchemaGroup;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.lib.conf.Configurations;
import org.apache.openjpa.slice.QueryTargetPolicy;
import org.apache.openjpa.slice.Slice;

/**
 *
 * @author fabio
 */
public class AzureQueryTargetPolicy implements QueryTargetPolicy {

    private SchemaGroup group = null;

    @Override
    public String[] getTargets(
            final String query,
            final Map<Object, Object> params,
            final String language,
            final List<String> slices,
            final Object context) {

        final Broker broker = (Broker) context;
        final AzureConfiguration conf = (AzureConfiguration) broker.getConfiguration();

        init(conf);

        NativeQueryInfo queryInfo = new NativeQueryInfo(query);

        List<String> tableNames = queryInfo.getTableNames();

        final List<String> result = new ArrayList<String>();

        for (String name : tableNames) {

            final Table table = group.findTable(QualifiedDBIdentifier.getPath(DBIdentifier.newTable(name)));
            final List<Federation> federations = conf.getFederations(table);

            if (federations.isEmpty()) {
                result.add("ROOT");
            } else {
                for (Federation federation : federations) {
                    Slice slice = conf.getSlice(federation.getName());
                    if (slice != null) {
                        result.add(slice.getName());
                    }
                }
            }
        }

        return result.toArray(new String[result.size()]);
    }

    private String[] getTableNames(final String fromClause) {
        final List<String> result = new ArrayList<String>();

        if (StringUtils.isBlank(fromClause)) {
            return new String[0];
        }

        for (String from : fromClause.split(",")) {
            String name = from.trim();
            result.add(name.substring(0, name.contains(" ") ? name.indexOf(" ") : name.length()));
        }

        return result.toArray(new String[result.size()]);
    }

    private void init(final AzureConfiguration conf) {
        if (group == null) {
            final MappingRepository repo = conf.getMappingRepositoryInstance();
            String action = "buildSchema";
            String props = Configurations.getProperties(action);
            action = Configurations.getClassName(action);

            final MappingTool tool = new AzureMappingTool(null, (JDBCConfiguration) conf, action, false);
            Configurations.configureInstance(tool, conf, props, "SynchronizeMappings");

            final Collection<Class<?>> classes = repo.loadPersistentTypes(false, this.getClass().getClassLoader());

            for (Class<?> cls : classes) {
                try {
                    tool.run(cls);
                } catch (IllegalArgumentException ignore) {
                    // ignore
                }
            }

            group = tool.getSchemaGroup();
        }
    }
}
