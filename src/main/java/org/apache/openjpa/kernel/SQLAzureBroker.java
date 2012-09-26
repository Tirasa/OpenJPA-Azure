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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.federation.jdbc.Federation;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.jdbc.meta.ClassMapping;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.util.ObjectId;
import org.apache.openjpa.utils.ObjectIdException;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzureBroker extends BrokerImpl {

    private transient Log _log = null;

    @Override
    public void initialize(
            final AbstractBrokerFactory factory,
            final DelegatingStoreManager sm,
            final boolean managed,
            final int connMode,
            final boolean fromDeserialization,
            final boolean fromWriteBehindCallback) {
        super.initialize(factory, sm, managed, connMode, fromDeserialization, fromWriteBehindCallback);
        _log = getConfiguration().getLog(OpenJPAConfiguration.LOG_RUNTIME);
    }

    @Override
    public Object find(final Object oid, final boolean validate, final FindCallbacks call) {

        Object res = null;

        try {
            res = super.find(oid, validate, call);
        } catch (RuntimeException ignore) {
            // ignore exception: table could not exist into the root federation
            if (_log.isTraceEnabled()) {
                _log.trace("Object " + oid + " does not exist into the root federation", ignore);
            }
        }

        if (res == null) {
            final ClassMapping classMapping =
                    ((SQLAzureConfiguration) getConfiguration()).getMappingRepositoryInstance().getMapping(
                    oid, SQLAzureConfiguration.class.getClassLoader(), true);

            final String tableName = classMapping.getTable().getFullIdentifier().getName();

            final Collection<Federation> federations =
                    ((SQLAzureConfiguration) getConfiguration()).getFederations(tableName);

            for (Iterator<Federation> iter = federations.iterator(); iter.hasNext() && res == null;) {
                final Federation federation = iter.next();

                try {
                    final Object objectId = parseObjectId(oid, federation.getRangeMappingName(tableName));

                    SQLAzureUtils.useFederation((Connection) getConnection(), federation, objectId);
                    res = super.find(oid, validate, call);

                } catch (SQLException ignore) {
                    if (_log.isTraceEnabled()) {
                        _log.trace("Error searching on '" + federation.getName() + "': " + ignore.getMessage());
                    }
                } catch (RuntimeException ignore) {
                    // ignore exception: table could not exist into the root federation
                    if (_log.isTraceEnabled()) {
                        _log.trace("Object " + oid + " does not exist into '" + federation.getName() + "'");
                    }
                } catch (Exception e) {
                    _log.warn("Error parsing object id " + oid);
                }
            }
        }

        return res;
    }

    @Override
    public Object attach(final Object obj, final boolean copyNew, final OpCallbacks call) {

        if (obj == null) {
            return null;
        }

        final ClassMapping classMapping = ((SQLAzureConfiguration) getConfiguration()).getMappingRepositoryInstance().
                getMapping(obj.getClass(), SQLAzureConfiguration.class.getClassLoader(), true);

        final String tableName = classMapping.getTable().getFullIdentifier().getName();

        final List<Federation> federations = ((SQLAzureConfiguration) getConfiguration()).getFederations(tableName);

        if (federations.isEmpty()) {
            return super.attach(obj, copyNew, call);
        } else {

            Object res = null;

            for (final Iterator<Federation> iter = federations.iterator(); iter.hasNext() && res == null;) {
                final Federation federation = iter.next();

                try {
                    final Object objectId = parseObjectId(getObjectId(obj), federation.getRangeMappingName(tableName));

                    SQLAzureUtils.useFederation((Connection) getConnection(), federation, objectId);
                    res = super.attach(obj, copyNew, call);

                } catch (Exception e) {
                    e.printStackTrace();
                    _log.warn("Error attaching object on '" + federation.getName() + "': " + e.getMessage());
                }
            }

            return res;
        }
    }

    private Object parseObjectId(final Object objectId, final String rangeMappingName)
            throws IllegalArgumentException, ObjectIdException {

        if (null == objectId) {
            throw new IllegalArgumentException("ObjectId " + objectId);
        }

        if (StringUtils.isBlank(rangeMappingName)) {
            throw new IllegalArgumentException("RangeMappingName " + rangeMappingName);
        }

        final Object res;

        if (objectId instanceof ObjectId) {
            try {

                final Object obj = ((ObjectId) objectId).getIdObject();

                final Method keyGetter = obj.getClass().getMethod(
                        "get" + org.springframework.util.StringUtils.capitalize(rangeMappingName), new Class[0]);

                res = keyGetter.invoke(obj, new Object[0]);

            } catch (Exception e) {
                throw new ObjectIdException(e.getMessage());
            }
        } else {
            res = objectId;
        }

        if (res == null) {
            throw new ObjectIdException("ObjectId " + objectId + " not found");
        }

        return res;
    }
}
