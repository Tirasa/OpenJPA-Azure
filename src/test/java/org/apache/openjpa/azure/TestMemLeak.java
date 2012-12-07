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
import org.apache.openjpa.azure.beans.BusinessRole;
import org.apache.openjpa.azure.beans.MPObject;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.azure.beans.PersonBINT;

public class TestMemLeak extends AbstractAzureTestCase {

    private static boolean initialized = false;

    @Override
    public void setUp() {
        super.setUp(new Class[]{}, CLEAR_TABLES);
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testMemLeak()
            throws Exception {
        for (int i = 0; i < 20; i++) {
            EntityManager em = emf.createEntityManager();
            em.clear();
            em.close();
            tearDown();
            setUp();
        }
    }
}
