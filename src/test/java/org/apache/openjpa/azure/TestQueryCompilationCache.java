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

import java.util.Map;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.test.SQLListenerTestCase;

public class TestQueryCompilationCache extends SQLListenerTestCase {

    @Override
    public void setUp() {
        super.setUp(
                "openjpa.QueryCompilationCache", "true",
                new Class[]{PObject.class}, CLEAR_TABLES);

        // check for issue OPENJPA-2301
        final OpenJPAEntityManager em = emf.createEntityManager();

        em.getTransaction().begin();
        em.createQuery("DELETE FROM PObject e");
        em.getTransaction().commit();
        em.close();
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testQueryCompilationCache()
            throws Exception {

        OpenJPAEntityManager em = emf.createEntityManager();

        final Map cache = emf.getConfiguration().getQueryCompilationCacheInstance();
        cache.clear();

        assertEquals(0, cache.size());

        OpenJPAQuery q = em.createQuery("SELECT o from PObject o");
        q.compile();
        em.close();

        // make sure that there's an entry in the cache now
        assertEquals(1, cache.size());

        // check for issue OPENJPA-2301
        em = emf.createEntityManager();

        em.getTransaction().begin();
        em.createQuery("DELETE FROM PObject e");
        em.getTransaction().commit();
        em.close();

        emf.close();
    }
}
