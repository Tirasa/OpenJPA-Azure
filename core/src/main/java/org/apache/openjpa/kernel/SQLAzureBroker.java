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
import org.apache.openjpa.federation.jdbc.FederationConfiguration;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzureBroker extends BrokerImpl {

    @Override
    public Object find(Object oid, boolean validate, FindCallbacks call) {
        // -------------------------
        // just for check configuration parameters
        // -------------------------
        getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).info(
                "Retrieve federations for " + this.getClass().getSimpleName());
        final String[] federations = ((FederationConfiguration) getConfiguration()).getFederationNames();
        for (String federation : federations) {
            getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME).info("Federation " + federation);
        }
        // -------------------------

        if (oid != null) {
            Connection conn = (Connection) getConnection();
            try {
                conn.createStatement().execute(
                        "USE FEDERATION " + SQLAzureUtils.federation + " (range_id = " + oid.toString() + ") "
                        + "WITH FILTERING=OFF, RESET");
            } catch (SQLException e) {
                getConfiguration().getLog("SQLAzure").error("Error getting federation ref", e);
            }
        }

        return super.find(oid, validate, call);
    }

    @Override
    public Object attach(Object obj, boolean copyNew, OpCallbacks call) {

        Object oid = getObjectId(obj);

        if (obj != null) {
            Connection conn = (Connection) getConnection();
            try {
                conn.createStatement().execute(
                        "USE FEDERATION " + SQLAzureUtils.federation + " (range_id = " + oid + ") "
                        + "WITH FILTERING=OFF, RESET");
            } catch (SQLException e) {
                getConfiguration().getLog("SQLAzure").error("Error getting federation ref", e);
            }
        }

        return super.attach(obj, copyNew, call);
    }
}
