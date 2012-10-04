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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration.RangeType;

public class Federation {

    private static final String DEFAULT_DISTRIBUTION_NAME = "range_id";

    private String name;

    private String distributionName;

    private RangeType rangeMappingType;

    private final Map<String, String> tables;

    public Federation() {
        tables = new HashMap<String, String>();
        rangeMappingType = RangeType.BIGINT;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getDistributionName() {
        return StringUtils.isBlank(distributionName) ? DEFAULT_DISTRIBUTION_NAME : distributionName;
    }

    public void setDistributionName(final String distributionName) {
        this.distributionName = distributionName;
    }

    public String getRangeMappingName(final String tableName) {
        return tables.get(tableName);
    }

    public RangeType getRangeMappingType() {
        return rangeMappingType;
    }

    public void setRangeMappingType(final RangeType rangeMappingType) {
        this.rangeMappingType = rangeMappingType;
    }

    public Set<String> getTables() {
        return tables.keySet();
    }

    public void addTable(final String table, final String rangeMappingName) {
        this.tables.put(table, rangeMappingName);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof Federation
                && (name == null && ((Federation) obj).getName() == null || ((Federation) obj).getName().equals(name));
    }

    @Override
    public int hashCode() {
        return name == null ? super.hashCode() : name.hashCode();
    }
}
