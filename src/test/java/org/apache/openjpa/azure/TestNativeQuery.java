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
import org.apache.openjpa.azure.beans.ConfBean;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.persistence.OpenJPAEntityManager;

public class TestNativeQuery extends AbstractAzureTestCase {

    private static boolean initialized = false;

    @Override
    public void setUp() {
        super.setUp(new Class[]{PObject.class, ConfBean.class}, CLEAR_TABLES);

        if (!initialized) {
            final EntityManager entityManager = emf.createEntityManager();

            entityManager.getTransaction().begin();
            entityManager.createQuery("DELETE FROM PObject p").executeUpdate();
            entityManager.createQuery("DELETE FROM ConfBean p").executeUpdate();
            entityManager.getTransaction().commit();

            try {
                entityManager.getTransaction().begin();
                entityManager.createNativeQuery("DROP INDEX PObject_Index ON PObject").executeUpdate();
                entityManager.getTransaction().commit();
            } catch (Exception ignore) {
                // ignore
                entityManager.getTransaction().rollback();
            }

            try {
                entityManager.getTransaction().begin();
                entityManager.createNativeQuery("DROP VIEW PObject_VIEW").executeUpdate();
                entityManager.getTransaction().commit();
            } catch (Exception ignore) {
                // ignore
                entityManager.getTransaction().rollback();
            }

            entityManager.close();

            initialized = true;
        }
    }

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testUDPLOCK() {

        final EntityManager entityManager = emf.createEntityManager();

        // --------------------
        // insert
        // --------------------
        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO ConfBean VALUES('testUDPLOCK-key', 'testUDPLOCK-value')");
        assertEquals(1, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        final List all =
                entityManager.createNativeQuery("SELECT * FROM ConfBean WITH (UPDLOCK) WHERE confKey='testUDPLOCK-key'").
                getResultList();
        assertEquals(1, all.size());

        entityManager.close();
    }

    public void testCreateIndex() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery("CREATE INDEX PObject_Index ON PObject(value)");
        assertEquals(0, query.executeUpdate());

        entityManager.getTransaction().commit();

        entityManager.close();
    }

    public void testCreateView() {

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery("CREATE VIEW PObject_VIEW AS SELECT * FROM PObject");
        assertEquals(0, query.executeUpdate());

        entityManager.getTransaction().commit();

        entityManager.close();
    }

    public void testSelect() {
        createIndependentObjects(10);

        final EntityManager entityManager = emf.createEntityManager();

        final List all = entityManager.createNativeQuery("SELECT * FROM PObject").getResultList();
        assertFalse(all.isEmpty());

        entityManager.close();
    }

    public void testInsertUpdateDelete() {
        final EntityManager entityManager = emf.createEntityManager();

        PObject pobj = new PObject();

        // --------------------
        // insert
        // --------------------
        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO PObject VALUES(" + pobj.getId() + ", " + pobj.getValue() + ")");
        assertEquals(2, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        query = entityManager.createNativeQuery("SELECT COUNT(id) FROM PObject");
        assertTrue(((Long) query.getSingleResult()) > 0);

        // --------------------
        // update
        // --------------------
        entityManager.getTransaction().begin();

        query = entityManager.createNativeQuery("UPDATE PObject SET value=10010 WHERE id=" + pobj.getId());
        assertEquals(2, query.executeUpdate());

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
        assertEquals(2, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        entityManager.close();
    }

    public void testCount() {
        final OpenJPAEntityManager entityManager = emf.createEntityManager();

        // --------------------
        // insert
        // --------------------
        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO ConfBean VALUES('testCount-key', 'testCount-value')");
        assertEquals(1, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        query = entityManager.createNativeQuery("SELECT COUNT(confKey) FROM ConfBean WHERE confKey='testCount-key'");
        assertEquals(1, ((Integer) query.getSingleResult()).intValue());

        entityManager.close();
    }

    public void testAnnidateCount() {
        final OpenJPAEntityManager entityManager = emf.createEntityManager();

        // --------------------
        // insert
        // --------------------
        entityManager.getTransaction().begin();

        Query query = entityManager.createNativeQuery(
                "INSERT INTO ConfBean VALUES('testAnnidateCount-key', 'testAnnidateCount-value')");
        assertEquals(1, query.executeUpdate());

        entityManager.getTransaction().commit();
        // --------------------

        query = entityManager.createNativeQuery(
                "SELECT COUNT(t.confKey) FROM (SELECT confKey FROM ConfBean WHERE confKey='testAnnidateCount-key') AS t");
        assertEquals(1, ((Integer) query.getSingleResult()).intValue());

        entityManager.close();
    }
}
