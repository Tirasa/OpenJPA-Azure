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
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.apache.openjpa.azure.beans.BusinessRole;
import org.apache.openjpa.azure.beans.ConfBean;
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
            entityManager.createQuery("DELETE FROM ConfBean p").executeUpdate();

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

    public void testInRoot() {
        final ConfBean bean = new ConfBean();
        bean.setKey("key");
        bean.setValue("value");

        EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();
        em.persist(bean);
        em.getTransaction().commit();
        
        em.clear();
        em.close();
        
        // start a new entity manager instance ....
        em = emf.createEntityManager();

        // chak read only contextually ...

        EntityTransaction t = em.getTransaction();
        t.begin();
        t.setRollbackOnly();

        ConfBean actual = em.find(ConfBean.class, "key");
        assertNotNull(actual);

        t.rollback();

        em.close();
    }

    public void testFindAll() {
        final EntityManager em = emf.createEntityManager();

        final TypedQuery<PObject> query = em.createQuery("SELECT e FROM PObject e", PObject.class);
        assertEquals(10, query.getResultList().size());

        em.close();
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
        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        final PObject pobj = new PObject();
        pobj.setValue(100000);

        em.persist(pobj);

        em.getTransaction().commit();

        final TypedQuery<PObject> query =
                em.createQuery("SELECT e FROM PObject e WHERE e.id = :id", PObject.class);

        query.setParameter("id", pobj.getId());
        assertEquals(1, query.getResultList().size());

        final PObject actual = em.find(PObject.class, pobj.getId());
        assertNotNull(actual);
        assertEquals(pobj.getId(), actual.getId());

        em.close();
    }

    public void testDistributedRollBack() {
        PObject pobj = new PObject();
        pobj.setValue(10000);

        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        em.persist(pobj);

        em.getTransaction().rollback();

        Query query = em.createNativeQuery("SELECT value FROM PObject WHERE id = " + pobj.getId());
        assertTrue(query.getResultList().isEmpty());

        em.close();
    }

    /**
     * Delete a single replicated object by EntityManager.remove().
     */
    public void testDelete() {
        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        final int before = count(PObject.class);

        final List<PObject> all = em.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        em.remove(all.get(0));

        em.getTransaction().commit();

        assertEquals(before - 1, count(PObject.class));

        assertNull(em.find(PObject.class, all.get(0).getId()));

        em.close();
    }

    /**
     * Update a single replicated object.
     */
    public void testUpdate() {
        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        final int before = count(PObject.class);

        final List<PObject> all = em.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());

        PObject obj = all.get(0);
        obj.setValue(1);

        em.merge(obj);

        em.getTransaction().commit();

        obj = em.find(PObject.class, obj.getId());

        assertEquals(1, obj.getValue());

        em.close();
    }

    public void testOrderBy() {
        final EntityManager em = emf.createEntityManager();

        final List<MPObject> ordered =
                em.createQuery("SELECT p FROM MPObject p ORDER BY p.value").getResultList();
        assertEquals(count(MPObject.class), ordered.size());

        final List<Integer> values = new ArrayList<Integer>(ordered.size());
        for (MPObject obj : ordered) {
            values.add(obj.getValue());
        }
        assertOrdering(values.toArray(new Integer[values.size()]), true);

        em.close();
    }

    public void testRepOrderBy() {
        final EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();

        final List<PObject> all = em.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        final Random randomGenerator = new Random();
        for (PObject obj : all) {
            obj.setValue(randomGenerator.nextInt(100));
            em.merge(obj);
        }

        em.getTransaction().commit();

        final List<PObject> ordered =
                em.createQuery("SELECT p FROM PObject p ORDER BY p.value DESC").getResultList();
        assertEquals(all.size(), ordered.size());

        final List<Integer> values = new ArrayList<Integer>(ordered.size());
        for (PObject obj : ordered) {
            values.add(obj.getValue());
        }
        assertOrdering(values.toArray(new Integer[values.size()]), false);

        em.close();
    }

    /**
     * Update in bulk by query.
     */
    public void testBulkUpdate() {
        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        final int count = count(PObject.class);
        final int updated = em.createQuery("UPDATE PObject p SET p.value = :value").
                setParameter("value", 5).executeUpdate();
        assertEquals(count * 2, updated);

        em.getTransaction().commit();

        assertEquals(count, count(PObject.class));

        final List<PObject> all = em.createQuery("SELECT p FROM PObject p").getResultList();
        assertFalse(all.isEmpty());
        for (PObject obj : all) {
            assertEquals(5, obj.getValue());
        }

        em.close();
    }

    public void testGroupByHaving() {
        final EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();

        PersonBINT person = new PersonBINT();
        person.setUsername("username1");
        person.setPassword("password1");
        person.setGender(Gender.M);
        em.persist(person);

        person = new PersonBINT();
        person.setUsername("username2");
        person.setPassword("password2");
        person.setGender(Gender.M);
        em.persist(person);

        person = new PersonBINT();
        person.setUsername("username3");
        person.setPassword("password3");
        person.setGender(Gender.F);
        em.persist(person);

        em.getTransaction().commit();

        List result = em.createQuery("SELECT c.gender, COUNT(c) FROM PersonBINT c "
                + "GROUP BY c.gender HAVING COUNT(c) > 1").getResultList();
        assertNotNull(result);
        assertEquals(1, result.size());

        em.close();
    }

    /**
     * Delete in bulk by query.
     */
    public void testBulkDelete() {
        final EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();

        final int count = count(PObject.class);
        final int deleted = em.createQuery("DELETE FROM PObject p").executeUpdate();
        assertEquals(count * 2, deleted);

        em.getTransaction().commit();
        em.close();

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
