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

import org.apache.openjpa.jdbc.conf.JDBCConfigurationImpl;
import org.apache.openjpa.lib.conf.BooleanValue;
import org.apache.openjpa.lib.conf.StringListValue;
import org.apache.openjpa.lib.conf.StringValue;
import org.apache.openjpa.lib.util.Localizer;

public class FederationConfigurationImpl extends JDBCConfigurationImpl implements FederationConfiguration {

    private static Localizer _loc = Localizer.forPackage(FederationConfiguration.class);

    public static final String PREFIX_FEDERATION = "openjpa.federation.";

    protected BooleanValue federatedPlugin;

    protected StringListValue namesPlugin;

    protected StringValue rangeMappingNamePlugin;

    public FederationConfigurationImpl() {
        super();

        getConfigurationLog().trace("Federation configuration initialization");

        federatedPlugin = addBoolean(PREFIX_FEDERATION + "Federated");
        federatedPlugin.setDefault("false");

        namesPlugin = addStringList(PREFIX_FEDERATION + "Names");

        rangeMappingNamePlugin = addString(PREFIX_FEDERATION + "RangeMappingName");
    }

    @Override
    public String[] getFederationNames() {
        return namesPlugin.get();
    }

    @Override
    public boolean isFederated() {
        return federatedPlugin.get() != null && federatedPlugin.get();
    }

    @Override
    public String getRangeMappingName() {
        return rangeMappingNamePlugin.get();
    }
}
