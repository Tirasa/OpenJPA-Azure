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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;

/**
 * Configuration class interface.
 */
public interface SQLAzureConfiguration extends JDBCConfiguration {

    public enum RangeType {

        BIGINT("bigint"),
        INT("int"),
        UNIQUEIDENTIFIER("uniqueidentifier"),
        VARBINARY("varbinary");

        /**
         * Type value string.
         */
        private String value;

        /**
         * Constructor.
         *
         * @param value type value string.
         */
        private RangeType(final String value) {
            this.value = value;
        }

        /**
         * Return type value string.
         *
         * @return type value string.
         */
        public String getValue() {
            return value;
        }
    };

    /**
     * Get all the federation names.
     *
     * @return list of federations.
     */
    Collection<Federation> getFederations();

    /**
     * Get all the federations for provided table.
     *
     * @param tableName table name.
     * @return list of federation for provided table.
     */
    List<Federation> getFederations(final String tableName);

    /**
     * Get column to be mapped on the range_id.
     *
     * @param federationName federation name.
     * @param tableName table name.
     * @return column name.
     */
    String getRangeMappingName(final String federationName, final String tableName);

    /**
     * Get type of the range_id (bigint, int, uniqueidentifier, varbinary).
     *
     * @param federationName federation name.
     * @return range_id type.
     */
    RangeType getRangeMappingType(final String federationName);

    /**
     * Get federated tables onto the given federation.
     *
     * @param federationName federation name.
     * @return set of tables.
     */
    Set<String> getFederatedTables(final String federationName);
}
