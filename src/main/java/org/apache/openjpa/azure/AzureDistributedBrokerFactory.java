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

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.conf.AzureConfigurationImpl;
import org.apache.openjpa.azure.jdbc.meta.AzureMappingTool;
import org.apache.openjpa.azure.kernel.AzureBroker;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.meta.MappingTool;
import org.apache.openjpa.lib.conf.ConfigurationProvider;
import org.apache.openjpa.lib.conf.Configurations;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.slice.DistributedBroker;
import org.apache.openjpa.slice.Slice;
import org.apache.openjpa.slice.jdbc.DistributedJDBCBrokerFactory;
import org.apache.openjpa.slice.jdbc.DistributedJDBCConfiguration;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;
import org.apache.openjpa.util.UserException;

public class AzureDistributedBrokerFactory extends DistributedJDBCBrokerFactory {

    private static final Localizer _loc = Localizer.forPackage(AzureDistributedBrokerFactory.class);

    private static final long serialVersionUID = 3111066668150403201L;

    public AzureDistributedBrokerFactory(AzureConfiguration conf) {
        super(conf);
    }

    @Override
    public DistributedBroker newBroker() {
        AzureBroker broker = new AzureBroker();
        return broker;
    }

    @Override
    protected DistributedJDBCStoreManager newStoreManager() {
        return new DistributedJDBCStoreManager(getConfiguration());
    }

    public static AzureDistributedBrokerFactory newInstance(ConfigurationProvider cp) {
        AzureConfigurationImpl conf = new AzureConfigurationImpl();
        cp.setInto(conf);
        return new AzureDistributedBrokerFactory(conf);
    }

    @Override
    public DistributedJDBCConfiguration getConfiguration() {
        return (AzureConfiguration) super.getConfiguration();
    }

    @Override
    protected void synchronizeMappings(ClassLoader loader) {
        final List<Slice> slices = getConfiguration().getSlices(Slice.Status.ACTIVE);
//        for (Slice slice : slices) {
//            synchronizeMappings(loader, slice);
//        }
    }

    protected void synchronizeMappings(final ClassLoader loader, final Slice slice) {

        final JDBCConfiguration conf =
                (JDBCConfiguration) Proxy.newProxyInstance(AzureSliceConfiguration.class.getClassLoader(),
                new Class<?>[]{AzureSliceConfiguration.class},
                new JDBCConfInterceptor(new AzureSliceConfigurationImpl(slice, (AzureConfiguration) getConfiguration())));

        String action = ((JDBCConfiguration) slice.getConfiguration()).getSynchronizeMappings();
        if (StringUtils.isEmpty(action)) {
            return;
        }

        final MappingRepository repo = conf.getMappingRepositoryInstance();
        final Collection<Class<?>> classes = repo.loadPersistentTypes(false, loader);

        if (classes.isEmpty()) {
            return;
        }

        String props = Configurations.getProperties(action);
        action = Configurations.getClassName(action);

        final MappingTool tool = new AzureMappingTool(conf, action, false);
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
