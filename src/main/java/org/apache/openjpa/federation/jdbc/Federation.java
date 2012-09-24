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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration.RangeType;

public class Federation {

    private String name;

    private RangeType rangeMappingType;

    private Map<String, String> tables;

    public Federation() {
        tables = new HashMap<String, String>();
        rangeMappingType = RangeType.BIGINT;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}