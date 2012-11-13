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
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.openjpa.azure.beans.MPObject;
import org.apache.openjpa.azure.beans.PObject;

public class TestJPQLAggregate extends AbstractAzureTestCase {

    private static long pobjMin = -1;

    private static long pobjMax = -1;

    private static long pobjAvg = 0;

    private static long pobjSum = 0;

    private static boolean initialized = false;

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    @Override
    public void setUp() {
        super.setUp(new Class[]{PObject.class, MPObject.class}, CLEAR_TABLES);

        if (!initialized) {
            final EntityManager entityManager = emf.createEntityManager();

            entityManager.getTransaction().begin();

            // Native query
            entityManager.createNativeQuery("DELETE FROM PObject").executeUpdate();

            // JPQL query
            entityManager.createQuery("DELETE FROM MPObject p").executeUpdate();

            for (int i = 9; i >= 0; i--) {

                MPObject mpobj = new MPObject();
                mpobj.setId(i);
                mpobj.setValue(i);

                entityManager.persist(mpobj);

                PObject pobj = new PObject();

                if (pobjMin < 0) {
                    pobjMin = pobj.getId();
                }

                pobjMax = pobj.getId();

                pobjSum += pobj.getId();

                entityManager.persist(pobj);
            }

            entityManager.getTransaction().commit();

            entityManager.clear();
            entityManager.close();

            pobjAvg = pobjSum / 10;

            initialized = true;
        }
    }

    public void testSum() {
        Long sum = (Long) emf.createEntityManager().createQuery("SELECT SUM(p.id) FROM MPObject p").getSingleResult();
        assertEquals(45, sum.longValue());
    }

    public void testRepSum() {
        Long sum = (Long) emf.createEntityManager().createQuery("SELECT SUM(p.id) FROM PObject p").getSingleResult();
        assertEquals(pobjSum, sum.longValue());
    }

    /**
     * Not implemented (#55).
     */
    public void NOTtestAvg() {
        Long avg = (Long) emf.createEntityManager().createQuery("SELECT AVG(p.id) FROM MPObject p").getSingleResult();
        assertEquals(4, avg.longValue());
    }

    /**
     * Not implemented (#55).
     */
    public void NOTtestRepAvg() {
        Long avg = (Long) emf.createEntityManager().createQuery("SELECT AVG(p.id) FROM PObject p").getSingleResult();
        assertEquals(pobjAvg, avg.longValue());
    }

    public void testMax() {
        Long max = (Long) emf.createEntityManager().createQuery("SELECT MAX(p.id) FROM MPObject p").getSingleResult();
        assertEquals(9, max.longValue());
    }

    public void testRepMax() {
        Long max = (Long) emf.createEntityManager().createQuery("SELECT MAX(p.id) FROM PObject p").getSingleResult();
        assertEquals(pobjMax, max.longValue());
    }

    public void testMin() {
        Long min = (Long) emf.createEntityManager().createQuery("SELECT MIN(p.id) FROM MPObject p").getSingleResult();
        assertEquals(0, min.longValue());
    }

    public void testRepMin() {
        Long min = (Long) emf.createEntityManager().createQuery("SELECT MIN(p.id) FROM PObject p").getSingleResult();
        assertEquals(pobjMin, min.longValue());
    }

    public void testCount() {
        final EntityManager entityManager = emf.createEntityManager();

        Query query = entityManager.createQuery("SELECT p FROM MPObject p");
        List all = query.getResultList();
        assertEquals(10, all.size());

        Long count = (Long) entityManager.createQuery("SELECT COUNT(p) FROM MPObject p").getSingleResult();
        assertEquals(10L, count.longValue());
    }

    public void testRepCount() {
        final EntityManager entityManager = emf.createEntityManager();

        Query query = entityManager.createQuery("SELECT p FROM PObject p");
        List all = query.getResultList();
        assertEquals(10, all.size());

        Long count = (Long) entityManager.createQuery("SELECT COUNT(p) FROM PObject p").getSingleResult();
        assertEquals(10L, count.longValue());
    }
}
