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
package org.apache.openjpa.azure.jdbc.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.ProductDerivation;
import org.apache.openjpa.azure.kernel.AzureBroker;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.lib.conf.StringListValue;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.slice.Slice;
import org.apache.openjpa.slice.jdbc.DistributedJDBCConfigurationImpl;

public class AzureConfigurationImpl extends DistributedJDBCConfigurationImpl implements AzureConfiguration {

    private static final long serialVersionUID = 8033042262237726572L;

    private static final Localizer _loc = Localizer.forPackage(AzureConfiguration.class);

    private final StringListValue federationsPlugin;

    private Map<String, Federation> federations = new HashMap<String, Federation>();

    private Map<String, List<Federation>> federatedTables = new HashMap<String, List<Federation>>();

    public AzureConfigurationImpl() {
        super();
        federationsPlugin = addStringList(ProductDerivation.PREFIX_AZURE + ".Federations");
        brokerPlugin.setString(AzureBroker.class.getName());
    }

    @Override
    public Collection<Federation> getFederations() {
        return federations.values();
    }

    @Override
    public String getDistributionName(final String federationName) {
        final Federation fed = federations.get(federationName);
        return fed == null ? null : fed.getDistributionName();
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
    public List<Federation> getFederations(final Table table) {
        final List<Federation> federations = new ArrayList<Federation>();

        if (table != null) {
            String tableName = table.getFullIdentifier().getName();

            federations.addAll(
                    federatedTables.get(tableName) == null ? Collections.EMPTY_LIST : federatedTables.get(tableName));

            for (ForeignKey fk : table.getForeignKeys()) {
                tableName = fk.getPrimaryKeyTable().getFullIdentifier().getName();
                federations.addAll(federatedTables.get(tableName) == null
                        ? Collections.EMPTY_LIST
                        : federatedTables.get(tableName));
            }
        }

        return federations;
    }

    @Override
    public List<Federation> getFederations(final String tableName) {
        return federatedTables.get(tableName) == null ? Collections.EMPTY_LIST : federatedTables.get(tableName);
    }

    @Override
    public void fromProperties(final Map original) {
        if (original.containsKey(DistributedJDBCConfigurationImpl.PREFIX_SLICE + "Names")) {
            super.fromProperties(original);
        }

        final Map<String, String> newProps = new HashMap<String, String>();

        for (Map.Entry<String, String> entry : ((Map<String, String>) original).entrySet()) {
            if (entry.getKey().startsWith(ProductDerivation.PREFIX_AZURE)) {
                newProps.put(entry.getKey(), entry.getValue());
            }
        }

        if (!newProps.isEmpty()) {
            for (String federationName : federationsPlugin.get()) {

                final Federation federation = new Federation();

                federation.setName(federationName);

                federation.setDistributionName(newProps.get(ProductDerivation.PREFIX_AZURE + "."
                        + federationName + ".DistributionName"));

                try {
                    federation.setRangeMappingType(RangeType.valueOf(
                            newProps.get(ProductDerivation.PREFIX_AZURE + "."
                            + federationName + ".RangeMappingType")));
                } catch (Exception e) {
                    federation.setRangeMappingType(RangeType.BIGINT);
                }

                final String fedTableNames = newProps.get(ProductDerivation.PREFIX_AZURE + "."
                        + federationName + ".Tables");

                final String[] tables = fedTableNames == null ? new String[0] : fedTableNames.split(",");

                for (String federatedTable : tables) {
                    String rangeMappingName = newProps.get(ProductDerivation.PREFIX_AZURE + "."
                            + federationName + "." + federatedTable + ".RangeMappingName");

                    if (StringUtils.isBlank(rangeMappingName)) {
                        getConfigurationLog().info(_loc.get("invalid-property", ProductDerivation.PREFIX_AZURE + "."
                                + federationName));
                        rangeMappingName = null;
                    }

                    federation.addTable(federatedTable, rangeMappingName);

                    List<Federation> owningFeds = federatedTables.get(federatedTable);
                    if (owningFeds == null) {
                        owningFeds = new ArrayList<Federation>();
                        federatedTables.put(federatedTable, owningFeds);
                    }
                    owningFeds.add(federation);
                }

                federations.put(federationName, federation);
            }
        }
    }

    @Override
    public boolean equals(final Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public Federation getFederation(final Slice slice) {
        return getFederation(slice.getName());
    }

    @Override
    public Federation getFederation(final String sliceName) {
        return federations.get(AzureUtils.getFederationName(sliceName));
    }
}
