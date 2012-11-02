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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.apache.openjpa.azure.beans.BusinessRole;
import org.apache.openjpa.azure.beans.Gender;
import org.apache.openjpa.azure.beans.MPObject;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.azure.beans.PersonBINT;

public class TestJPQLBasic extends AbstractAzureTestCase {

    private static boolean initialized = false;

    @Override
    public void setUp() {
        super.setUp(new Class[]{PObject.class, MPObject.class, PersonBINT.class, BusinessRole.class}, CLEAR_TABLES);

        if (!initialized) {
            final EntityManager entityManager = emf.createEntityManager();

            entityManager.getTransaction().begin();

            entityManager.createNativeQuery("DELETE FROM Membership").executeUpdate();
            entityManager.createQuery("DELETE FROM PersonBINT p").executeUpdate();
            entityManager.createQuery("DELETE FROM BusinessRole p").executeUpdate();
            entityManager.createQuery("DELETE FROM MPObject p").executeUpdate();
            entityManager.createQuery("DELETE FROM PObject p").executeUpdate();

            final Random randomGenerator = new Random();

            for (int i = 9; i >= 0; i--) {
                MPObject mpobj = new MPObject();
                mpobj.setId(i);
                mpobj.setValue(randomGenerator.nextInt(100));

                entityManager.persist(mpobj);

                PObject pobj = new PObject();
                pobj.setValue(randomGenerator.nextInt(100));

                entityManager.persist(new PObject());
            }

            entityManager.getTransaction().commit();
            entityManager.clear();
            entityManager.close();

            initialized = true;
        }
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testFindAll() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.getTransaction().commit();

        final TypedQuery<PObject> query = entityManager.createQuery("SELECT e FROM PObject e", PObject.class);
        assertEquals(20, query.getResultList().size());
    }

    public void testMultiFindAll() {
        final EntityManager em1 = emf.createEntityManager();

        em1.getTransaction().begin();

        em1.persist(new PObject());
        em1.persist(new PObject());

        em1.getTransaction().commit();

        assertFalse(em1.createQuery("SELECT e FROM PObject e", PObject.class).getResultList().isEmpty());

        final EntityManager em2 = emf.createEntityManager();

        assertFalse(em2.createQuery("SELECT e FROM PObject e", PObject.class).getResultList().isEmpty());

        em2.close();

        assertFalse(em1.createQuery("SELECT e FROM PObject e", PObject.class).getResultList().isEmpty());

        em1.close();
    }

    public void testFindOne() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        final PObject pobj = new PObject();
        pobj.setValue(100000);

        entityManager.persist(pobj);

        entityManager.getTransaction().commit();

        final TypedQuery<PObject> query =
                entityManager.createQuery("SELECT e FROM PObject e WHERE e.id = :id", PObject.class);

        query.setParameter("id", pobj.getId());
        assertEquals(2, query.getResultList().size());

        final PObject actual = entityManager.find(PObject.class, pobj.getId());
        assertNotNull(actual);
        assertEquals(pobj.getId(), actual.getId());
    }

    public void testDistributedRollBack() {
        PObject pobj = new PObject();
        pobj.setValue(10000);

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.persist(pobj);

        entityManager.getTransaction().rollback();

        Query query = entityManager.createNativeQuery("SELECT value FROM PObject WHERE id = " + pobj.getId());
        assertTrue(query.getResultList().isEmpty());
    }

    /**
     * Delete a single object by EntityManager.remove().
     */
    public void testDelete() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();
        final int before = count(PObject.class);

        final List<PObject> all = entityManager.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        entityManager.remove(all.get(0));
        entityManager.getTransaction().commit();

        // Deteted two object: one per federation.
        assertEquals(before - 2, count(PObject.class));
    }

    public void testOrderBy() {
        final EntityManager entityManager = emf.createEntityManager();

        final List<MPObject> ordered =
                entityManager.createQuery("SELECT p FROM MPObject p ORDER BY p.value").getResultList();
        assertEquals(count(MPObject.class), ordered.size());

        final List<Integer> values = new ArrayList<Integer>(ordered.size());
        for (MPObject obj : ordered) {
            values.add(obj.getValue());
        }
        assertOrdering(values.toArray(new Integer[values.size()]), true);
    }

    public void testRepOrderBy() {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();

        final List<PObject> all = entityManager.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        final Random randomGenerator = new Random();
        for (PObject obj : all) {
            obj.setValue(randomGenerator.nextInt(100));
            entityManager.merge(obj);
        }

        entityManager.getTransaction().commit();

        final List<PObject> ordered =
                entityManager.createQuery("SELECT p FROM PObject p ORDER BY p.value DESC").getResultList();
        assertEquals(all.size(), ordered.size());

        final List<Integer> values = new ArrayList<Integer>(ordered.size());
        for (PObject obj : ordered) {
            values.add(obj.getValue());
        }
        assertOrdering(values.toArray(new Integer[values.size()]), false);
    }

    /**
     * Update in bulk by query.
     */
    public void testBulkUpdate() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();
        final int count = count(PObject.class);
        final int updated = entityManager.createQuery("UPDATE PObject p SET p.value = :value").
                setParameter("value", 5).executeUpdate();
        assertEquals(count, updated);
        entityManager.getTransaction().commit();

        assertEquals(count, count(PObject.class));

        final List<PObject> all = entityManager.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        for (PObject obj : all) {
            assertEquals(5, obj.getValue());
        }
    }

    public void testGroupByHaving() {
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();

        PersonBINT person = new PersonBINT();
        person.setUsername("username1");
        person.setPassword("password1");
        person.setGender(Gender.M);
        entityManager.persist(person);

        person = new PersonBINT();
        person.setUsername("username2");
        person.setPassword("password2");
        person.setGender(Gender.M);
        entityManager.persist(person);

        person = new PersonBINT();
        person.setUsername("username3");
        person.setPassword("password3");
        person.setGender(Gender.F);
        entityManager.persist(person);

        entityManager.getTransaction().commit();

        List result = entityManager.createQuery("SELECT c.gender, COUNT(c) FROM PersonBINT c "
                + "GROUP BY c.gender HAVING COUNT(c) > 1").getResultList();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    /**
     * Delete in bulk by query.
     */
    public void testBulkDelete() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();
        final int count = count(PObject.class);
        final int deleted = entityManager.createQuery("DELETE FROM PObject p").executeUpdate();
        assertEquals(count, deleted);
        entityManager.getTransaction().commit();

        assertEquals(0, count(PObject.class));
    }

    private void assertOrdering(Comparable[] items, boolean ascending) {
        assertNotNull(items);
        assertTrue(items.length > 0);
        for (int i = 1; i < items.length; i++) {
            if (ascending) {
                assertTrue(items[i].compareTo(items[i - 1]) >= 0);
            } else {
                assertTrue(items[i].compareTo(items[i - 1]) <= 0);
            }
        }
    }
}
