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
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

public class TestBasic extends AbstractAzureTestCase {

    /**
     * Specify persistence unit name as System property
     * <code>-Dunit</code> or use the default value as
     * <code>"azure"</code>.
     */
    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure");
    }

    @Override
    public void setUp() {
        super.setUp(PObject.class, Person.class, Address.class, Country.class, CLEAR_TABLES);
    }

    /**
     * Persist num independent objects.
     */
    protected List<PObject> createIndependentObjects(final int num) {
        final List<PObject> pcs = new ArrayList<PObject>();
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        for (int i = 0; i < num; i++) {
            final PObject pobj = new PObject();
            pcs.add(pobj);
            entityManager.persist(pobj);
            pobj.setValue(10 + i);
        }
        entityManager.getTransaction().commit();
        entityManager.clear();
        return pcs;
    }

    /**
     * Create a single object.
     */
    protected PObject createIndependentObject() {
        return createIndependentObjects(1).get(0);
    }

    /**
     * Delete a single object by EntityManager.remove().
     */
    public void testDelete() {
        createIndependentObjects(10);

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

    public void testNativeSelect() {
        createIndependentObjects(10);

        final EntityManager entityManager = emf.createEntityManager();

        final List all = entityManager.createNativeQuery("SELECT * FROM PObject").getResultList();
        assertFalse(all.isEmpty());
    }

    public void testFindOne() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        PObject pobj = new PObject();
        pobj.setValue(100000);

        entityManager.persist(pobj);

        entityManager.getTransaction().commit();

        final TypedQuery<PObject> query =
                entityManager.createQuery("SELECT e FROM PObject e WHERE e.id = :id", PObject.class);
        query.setParameter("id", pobj.getId());
        assertEquals(2, query.getResultList().size());

        PObject actual = entityManager.find(PObject.class, pobj.getId());
        assertNotNull(actual);
        assertEquals(pobj.getId(), actual.getId());
    }

    public void testNativeInsertUpdateDelete() {

        final EntityManager entityManager = emf.createEntityManager();

        PObject pobj = new PObject();

        // --------------------
        // insert
        // --------------------
        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO PObject VALUES(" + pobj.getId() + ", " + pobj.getValue() + ")");
        // inserted two objects: one per federation
        assertEquals(1, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        // --------------------
        // update
        // --------------------
        entityManager.getTransaction().begin();

        query = entityManager.createNativeQuery("UPDATE PObject SET value=10010 WHERE id=" + pobj.getId());
        // updated two objects: one per federation
        assertEquals(1, query.executeUpdate());

        final List all = entityManager.createNativeQuery(
                "SELECT value FROM PObject WHERE id=" + pobj.getId()).getResultList();
        assertFalse(all.isEmpty());

        for (int value : (List<Integer>) all) {
            assertEquals(10010, value);
        }

        entityManager.getTransaction().commit();
        // --------------------

        // --------------------
        // delete
        // --------------------
        entityManager.getTransaction().begin();

        query = entityManager.createNativeQuery("DELETE FROM PObject WHERE id=" + pobj.getId());
        // deleted two objects: one per federation
        assertEquals(1, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------
    }

    public void testCrossRollBack() {
        PObject pobj = new PObject();

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO PObject VALUES(" + pobj.getId() + ", " + pobj.getValue() + ")");
        // inserted two objects: one per federation
        assertEquals(1, query.executeUpdate());

        List all = entityManager.createNativeQuery("SELECT value FROM PObject WHERE id=" + pobj.getId()).getResultList();
        assertEquals(2, all.size());

        entityManager.getTransaction().rollback();

        all = entityManager.createNativeQuery("SELECT value FROM PObject WHERE id=" + pobj.getId()).getResultList();
        assertTrue(all.isEmpty());
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
}
