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

import javax.persistence.EntityManager;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.persistence.test.SingleEMFTestCase;

public abstract class AbstractAzureTestCase extends SingleEMFTestCase {

    @Override
    protected void setUp(final Object... props) {
        super.setUp(props);
        assertTrue(emf.getClass().getName() + " is not a SQL Azure configuration. "
                + "Check that BrokerFactory for the persistence unit is set to azure",
                emf.getConfiguration() instanceof SQLAzureConfiguration);

    }

    @Override
    public int count(final Class<?> type) {
        final EntityManager entityManager = emf.createEntityManager();
        // TODO: when COUNT() will be implemented change here accordingly
        final String query = "SELECT p FROM " + type.getSimpleName() + " p";
        return entityManager.createQuery(query).getResultList().size();
    }
}
