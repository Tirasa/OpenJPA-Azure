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
package org.apache.openjpa.federation.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.jdbc.conf.JDBCConfigurationImpl;
import org.apache.openjpa.lib.conf.StringListValue;
import org.apache.openjpa.lib.util.Localizer;

public class SQLAzureConfigurationImpl extends JDBCConfigurationImpl implements SQLAzureConfiguration {

    private static Localizer _loc = Localizer.forPackage(SQLAzureConfiguration.class);

    public static final String PREFIX_FEDERATION = "openjpa.sqlazure.";

    private static final long serialVersionUID = 8033042262237726572L;

    private final StringListValue federationsPlugin;

    private Map<String, Federation> federations = new HashMap<String, Federation>();

    private Map<String, List<Federation>> federatedTables = new HashMap<String, List<Federation>>();

    public SQLAzureConfigurationImpl() {
        super();

        federationsPlugin = addStringList(PREFIX_FEDERATION + "Federations");
    }

    @Override
    public Collection<Federation> getFederations() {
        return federations.values();
    }

    @Override
    public String getRangeMappingName(final String federationName, final String tableName) {
        final Federation fed = federations.get(federationName);
        return fed == null ? null : fed.getRangeMappingName(tableName);
    }

    @Override
    public RangeType getRangeMappingType(final String federationName) {
        try {
            return federations.get(federationName).getRangeMappingType();
        } catch (Exception e) {
            return RangeType.BIGINT;
        }
    }

    @Override
    public Set<String> getFederatedTables(final String federationName) {
        final Federation fed = federations.get(federationName);
        return fed == null ? Collections.EMPTY_SET : fed.getTables();
    }

    @Override
    public List<Federation> getFederations(final String tableName) {
        return federatedTables.get(tableName) == null ? Collections.EMPTY_LIST : federatedTables.get(tableName);
    }

    @Override
    public void fromProperties(final Map original) {
        super.fromProperties(original);

        Map<String, String> newProps = new HashMap<String, String>();

        for (String key : (Set<String>) original.keySet()) {
            if (key.startsWith(PREFIX_FEDERATION)) {
                newProps.put(key, (String) original.get(key));
            }
        }

        if (!newProps.isEmpty()) {
            for (String federationName : federationsPlugin.get()) {
                final Federation federation = new Federation();

                federation.setName(federationName);

                try {

                    federation.setRangeMappingType(RangeType.valueOf(
                            (String) newProps.get(PREFIX_FEDERATION + federationName + ".RangeMappingType")));

                } catch (Exception e) {
                    federation.setRangeMappingType(RangeType.BIGINT);
                }

                final String fedTableNames = (String) newProps.get(PREFIX_FEDERATION + federationName + ".Tables");

                final String[] tables = fedTableNames == null ? new String[0] : fedTableNames.split(",");

                for (String federatedTable : tables) {

                    String rangeMappingName = newProps.get(
                            PREFIX_FEDERATION + federationName + "." + federatedTable + ".RangeMappingName");

                    if (StringUtils.isBlank(rangeMappingName)) {
                        getConfigurationLog().info(_loc.get("invalid-property", PREFIX_FEDERATION + federationName));
                        rangeMappingName = null;
                    }

                    federation.addTable(federatedTable, rangeMappingName);

                    List<Federation> federations = federatedTables.get(federatedTable);

                    if (federations == null) {
                        federations = new ArrayList<Federation>();
                        federatedTables.put(federatedTable, federations);
                    }

                    federations.add(federation);
                }

                federations.put(federationName, federation);
            }
        }
    }
}
