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
import org.apache.openjpa.datacache.DataCacheManager;
import org.apache.openjpa.datacache.DataCacheStoreManager;
import org.apache.openjpa.enhance.PersistenceCapable;
import org.apache.openjpa.kernel.DelegatingStoreManager;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.persistence.EntityManagerImpl;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.test.SQLListenerTestCase;

public class TestDataCache extends SQLListenerTestCase {

    private static final String CACHE_NAME = "QueryCacheName";

    private OpenJPAEntityManager em;

    private DataCacheStoreManager dsm;

    private DataCacheManager dcm;

    private OpenJPAStateManager sm;

    private Object oid;

    @Override
    public void setUp() {
        super.setUp(
                "openjpa.DataCache", "true(EnableStatistics=true)",
                "openjpa.QueryCache", "true(name=" + CACHE_NAME + ")",
                "openjpa.RemoteCommitProvider", "sjvm",
                PObject.class);

        em = emf.createEntityManager();

        dcm = emf.getConfiguration().getDataCacheManagerInstance();
        dsm = (DataCacheStoreManager) ((DelegatingStoreManager) ((EntityManagerImpl) em).getBroker().getStoreManager()).
                getDelegate();

        em.getTransaction().begin();
        em.createQuery("DELETE FROM PObject e").executeUpdate();
        PObject pobj = new PObject();
        em.persist(pobj);
        oid = em.getObjectId(pobj);
        em.getTransaction().commit();

        dcm.stopCaching(PObject.class.getName());

        sm = (OpenJPAStateManager) ((PersistenceCapable) pobj).pcGetStateManager();
    }

    public void tearDown()
            throws Exception {
        dcm.startCaching(PObject.class.getName());
        em.close();

        super.tearDown();
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testExists() {
        dsm.exists(sm, null);
    }

    public void testsyncVersion() {
        dsm.syncVersion(sm, null);
    }

    public void testCache() {
        em.close();
        
        em = emf.createEntityManager();
        
        final List<PObject> objs = em.createQuery("SELECT o FROM PObject o").getResultList();
        assertEquals(1, objs.size());

        assertTrue(OpenJPAPersistence.cast(emf).getStoreCache().contains(PObject.class, oid));

        em.getTransaction().begin();
        em.createQuery("DELETE FROM PObject e").executeUpdate();
        em.getTransaction().commit();
        em.close();
        
        em = emf.createEntityManager();
        assertFalse(OpenJPAPersistence.cast(emf).getStoreCache().contains(PObject.class, oid));
    }
}
