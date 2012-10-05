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
import org.apache.openjpa.azure.beans.PObject;

public class TestNativeQuery extends AbstractAzureTestCase {

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testSelect() {
        createIndependentObjects(10);

        final EntityManager entityManager = emf.createEntityManager();

        final List all = entityManager.createNativeQuery("SELECT * FROM PObject").getResultList();
        assertFalse(all.isEmpty());
    }

    public void NOTtestInsertUpdateDelete() {
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
}
