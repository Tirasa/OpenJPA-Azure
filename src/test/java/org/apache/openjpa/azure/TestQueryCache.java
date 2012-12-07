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

import java.util.List;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.datacache.ConcurrentQueryCache;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.test.SQLListenerTestCase;

public class TestQueryCache extends SQLListenerTestCase {

    private static final String CACHE_NAME = "QueryCacheName";

    @Override
    public void setUp() {
        super.setUp(
                "openjpa.QueryCache", "true(name=" + CACHE_NAME + ")",
                "openjpa.RemoteCommitProvider", "sjvm",
                PObject.class);

        final OpenJPAEntityManager em = emf.createEntityManager();

        em.getTransaction().begin();
        em.persist(new PObject());
        em.getTransaction().commit();

        em.clear();
        em.close();
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testCache() {
        final OpenJPAEntityManager em = emf.createEntityManager();

        List<PObject> objs = em.createQuery("SELECT o FROM PObject o").getResultList();
        assertEquals(1, objs.size());

        resetSQL();

        objs = em.createQuery("SELECT o FROM PObject o").getResultList();
        assertEquals(1, objs.size());

        objs = em.createQuery("SELECT o FROM PObject o").getResultList();
        assertEquals(1, objs.size());

        assertEquals(0, getSQLCount());

        em.close();
    }

    public void testName() {
        ConcurrentQueryCache qCache =
                (ConcurrentQueryCache) emf.getConfiguration().getDataCacheManagerInstance().getSystemQueryCache();
        assertNotNull(qCache);
        assertEquals(CACHE_NAME, qCache.getName());
    }
}
