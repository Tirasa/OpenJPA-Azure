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

import org.apache.openjpa.jdbc.conf.JDBCConfiguration;

/**
 * Configuration class interface.
 */
public interface FederationConfiguration extends JDBCConfiguration {

    public enum RangeType {

        BIGINT("bigint"),
        INT("int"),
        UNIQUEIDENTIFIER("uniqueidentifier"),
        VARBINARY("varbinary");

        private String value;

        private RangeType(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    };

    /**
     * Get federation names.
     *
     * @return array of federation names.
     */
    String[] getFederationNames();

    /**
     * Check if is a federated DB.
     *
     * @return TRUE if the SQLAzure DB is federated; FALSE otherwise.
     */
    boolean isFederated();

    /**
     * Get column to be mapped on the range_id.
     *
     * @return column name.
     */
    public String getRangeMappingName();

    /**
     * Get type of the range_id (bigint, int, uniqueidentifier, varbinary).
     *
     * @return range_id type.
     */
    public RangeType getRangeMappingType();
}
