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
import java.util.Map;
import org.apache.openjpa.azure.beans.ConfBean;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.test.SQLListenerTestCase;

public class TestQueryCompilationCache extends SQLListenerTestCase {

    private static boolean initialized = false;

    @Override
    public void setUp() {
        super.setUp(
                "openjpa.QueryCompilationCache", "true",
                new Class[]{PObject.class, ConfBean.class}, CLEAR_TABLES);

        if (!initialized) {
            // check for issue OPENJPA-2301
            final OpenJPAEntityManager em = emf.createEntityManager();

            em.getTransaction().begin();
            em.createQuery("DELETE FROM PObject e").executeUpdate();
            em.createQuery("DELETE FROM ConfBean e").executeUpdate();

            ConfBean confBean = new ConfBean();
            confBean.setKey("chechForQueryResult-key");
            confBean.setValue("chechForQueryResult-value");

            em.persist(confBean);

            em.getTransaction().commit();
            em.close();

            initialized = true;
        }
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testQueryCompilation()
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

    public void testForQueryResult() {
        OpenJPAEntityManager em = emf.createEntityManager();

        em.getTransaction().begin();
        List<ConfBean> res = em.createQuery("SELECT e FROM ConfBean e").getResultList();
        assertEquals(1, res.size());
        assertEquals("chechForQueryResult-value", res.get(0).getValue());
        em.getTransaction().commit();
        em.close();

        em = emf.createEntityManager();

        em.getTransaction().begin();
        res = em.createQuery("SELECT e FROM ConfBean e").getResultList();
        assertEquals(1, res.size());
        assertEquals("chechForQueryResult-value", res.get(0).getValue());
        em.getTransaction().commit();
        em.close();
    }
}
