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

import java.util.Collection;
import java.util.List;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfigurationImpl;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.slice.Slice;

public class AzureSliceConfigurationImpl extends JDBCConfigurationImpl implements AzureSliceConfiguration {

    private final AzureConfiguration globalConf;

    private final Slice slice;

    public AzureSliceConfigurationImpl(final Slice slice, final AzureConfiguration conf) {
        this.globalConf = conf;
        this.slice = slice;
    }

    @Override
    public List<Federation> getFederations(final Table table) {
        return globalConf.getFederations(table);
    }

    @Override
    public Federation getFederation(final String sliceName) {
        return globalConf.getFederation(sliceName);
    }

    @Override
    public JDBCConfiguration getSliceConf() {
        return (JDBCConfiguration) slice.getConfiguration();
    }

    @Override
    public List<Federation> getFederations(final String tableName) {
        return globalConf.getFederations(tableName);
    }

    @Override
    public Collection<Federation> getFederations() {
        return globalConf.getFederations();
    }

    @Override
    public AzureConfiguration getGlobalConf() {
        return globalConf;
    }
}
