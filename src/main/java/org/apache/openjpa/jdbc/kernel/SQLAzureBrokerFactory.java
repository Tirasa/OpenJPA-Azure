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
package org.apache.openjpa.jdbc.kernel;

import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.federation.jdbc.SQLAzureConfigurationImpl;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.meta.MappingTool;
import org.apache.openjpa.jdbc.meta.SQLAzureMappingTool;
import org.apache.openjpa.kernel.StoreManager;
import org.apache.openjpa.lib.conf.ConfigurationProvider;
import org.apache.openjpa.lib.conf.Configurations;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.util.UserException;

public class SQLAzureBrokerFactory extends JDBCBrokerFactory {

    private static final long serialVersionUID = 1641615009581406164L;

    private static final Localizer _loc = Localizer.forPackage(SQLAzureBrokerFactory.class);

    public SQLAzureBrokerFactory(SQLAzureConfiguration conf) {
        super(conf);
    }

    @Override
    protected StoreManager newStoreManager() {
        return new SQLAzureStoreManager();
    }

    public static JDBCBrokerFactory newInstance(ConfigurationProvider cp) {
        SQLAzureConfiguration conf = new SQLAzureConfigurationImpl();
        cp.setInto(conf);
        return new SQLAzureBrokerFactory(conf);
    }

    @Override
    protected void synchronizeMappings(ClassLoader loader, JDBCConfiguration conf) {
        String action = conf.getSynchronizeMappings();
        if (StringUtils.isEmpty(action)) {
            return;
        }

        MappingRepository repo = conf.getMappingRepositoryInstance();
        Collection<Class<?>> classes = repo.loadPersistentTypes(false, loader);
        if (classes.isEmpty()) {
            return;
        }

        final String props = Configurations.getProperties(action);
        action = Configurations.getClassName(action);
        SQLAzureMappingTool tool = new SQLAzureMappingTool(conf, action, false);
        Configurations.configureInstance(tool, conf, props, "SynchronizeMappings");

        // initialize the schema
        for (Class<?> cls : classes) {
            try {
                tool.run(cls);
            } catch (IllegalArgumentException iae) {
                throw new UserException(_loc.get("bad-synch-mappings", action, Arrays.asList(MappingTool.ACTIONS)));
            }
        }

        tool.record();
    }
}
