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
package org.apache.openjpa.azure.jdbc.kernel;

import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.conf.AzureConfigurationImpl;
import org.apache.openjpa.azure.jdbc.meta.AzureMappingTool;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.AzureStoreManager;
import org.apache.openjpa.jdbc.kernel.JDBCBrokerFactory;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.meta.MappingTool;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.lib.conf.ConfigurationProvider;
import org.apache.openjpa.lib.conf.Configurations;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.util.UserException;

public class AzureBrokerFactory extends JDBCBrokerFactory {

    private static final long serialVersionUID = 1641615009581406164L;

    private static final Localizer _loc = Localizer.forPackage(AzureBrokerFactory.class);

    public AzureBrokerFactory(final AzureConfiguration conf) {
        super(conf);
    }

    @Override
    protected StoreManager newStoreManager() {
        return new AzureStoreManager();
    }

    public static JDBCBrokerFactory newInstance(final ConfigurationProvider provider) {
        final AzureConfiguration conf = new AzureConfigurationImpl();
        provider.setInto(conf);
        return new AzureBrokerFactory(conf);
    }

    @Override
    protected void synchronizeMappings(final ClassLoader loader, final JDBCConfiguration conf) {
        String action = conf.getSynchronizeMappings();
        if (StringUtils.isEmpty(action)) {
            return;
        }

        final MappingRepository repo = conf.getMappingRepositoryInstance();
        final Collection<Class<?>> classes = repo.loadPersistentTypes(false, loader);
        if (classes.isEmpty()) {
            return;
        }

        final String props = Configurations.getProperties(action);
        action = Configurations.getClassName(action);
        final AzureMappingTool tool = new AzureMappingTool(conf, action, false);
        Configurations.configureInstance(tool, conf, props, "SynchronizeMappings");

        // initialize the schema
        for (Class<?> cls : classes) {
            try {
                tool.run(cls);
            } catch (IllegalArgumentException iae) {
                throw new UserException(_loc.get("bad-synch-mappings", action, Arrays.asList(MappingTool.ACTIONS)), iae);
            }
        }

        tool.record();
    }
}
