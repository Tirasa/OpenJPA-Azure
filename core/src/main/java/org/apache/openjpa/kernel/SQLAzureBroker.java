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
package org.apache.openjpa.kernel;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzureBroker extends BrokerImpl {

    @Override
    public Object find(Object oid, boolean validate, FindCallbacks call) {

        if (oid != null) {
            try {
                SQLAzureUtils.useFederation((Connection) getConnection(), oid.toString());
            } catch (SQLException e) {
                getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).error("Error using federation", e);
            }
        }

        return super.find(oid, validate, call);
    }

    @Override
    public Object attach(Object obj, boolean copyNew, OpCallbacks call) {

        final Object oid = getObjectId(obj);

        if (obj != null) {
            try {
                SQLAzureUtils.useFederation((Connection) getConnection(), oid.toString());
            } catch (SQLException e) {
                getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).error("Error using federation", e);
            }
        }

        return super.attach(obj, copyNew, call);
    }
}
