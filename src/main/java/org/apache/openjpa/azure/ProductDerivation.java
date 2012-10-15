/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.openjpa.azure.jdbc.kernel.AzureBrokerFactory;
import org.apache.openjpa.azure.jdbc.sql.AzureSQLFactory;
import org.apache.openjpa.azure.kernel.AzureBroker;
import org.apache.openjpa.conf.OpenJPAProductDerivation;
import org.apache.openjpa.jdbc.sql.AzureDictionary;
import org.apache.openjpa.lib.conf.AbstractProductDerivation;
import org.apache.openjpa.lib.conf.Configuration;

/**
 * Derives configuration for Windows Azure. Introduces a specialized BrokerFactory aliased as
 * <code>azure</code>. All Azure specific configuration is prefixed as
 * <code>openjpa.azure.*.*</code>
 */
public class ProductDerivation extends AbstractProductDerivation implements OpenJPAProductDerivation {

    /**
     * Prefix for all Azure-specific configuration properties.
     */
    public static final String PREFIX_AZURE = "openjpa.azure";

    public static final Map<String, String> AZURE_CONF_MAP = new HashMap<String, String>();

    static {
//        AZURE_CONF_MAP.put("openjpa.jdbc.UpdateManager", AzureUpdateManager.class.getName());
        AZURE_CONF_MAP.put("openjpa.jdbc.SQLFactory", AzureSQLFactory.class.getName());
        AZURE_CONF_MAP.put("openjpa.jdbc.DBDictionary", AzureDictionary.class.getName());
        AZURE_CONF_MAP.put("openjpa.BrokerImpl", AzureBroker.class.getName());
    }

    @Override
    public void putBrokerFactoryAliases(final Map<String, String> aliases) {
        aliases.put("azure", AzureBrokerFactory.class.getName());
    }

    @Override
    public boolean beforeConfigurationLoad(final Configuration conf) {
        conf.fromProperties(AZURE_CONF_MAP);

        return true;
    }

    @Override
    public String getConfigurationPrefix() {
        return PREFIX_AZURE;
    }

    @Override
    public int getType() {
        return TYPE_STORE;
    }
}
