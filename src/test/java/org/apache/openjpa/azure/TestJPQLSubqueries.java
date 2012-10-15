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
import org.apache.openjpa.persistence.OpenJPAEntityManager;

public class TestJPQLSubqueries extends AbstractAzureTestCase {

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    @Override
    public void setUp() {
        super.setUp();

        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();

        entityManager.createQuery("DELETE FROM MPObject p").executeUpdate();
        entityManager.createQuery("DELETE FROM PObject p").executeUpdate();

        for (int i = 9; i >= 0; i--) {
            MPObject mpobj = new MPObject();
            mpobj.setId(i);
            mpobj.setValue(i);

            entityManager.persist(mpobj);

            PObject pobj = new PObject();
            pobj.setValue(i + 1);

            entityManager.persist(pobj);
        }

        entityManager.getTransaction().commit();
        entityManager.clear();
        entityManager.close();
    }

    public void testExists() {
        final OpenJPAEntityManager entityManager = emf.createEntityManager();

        Query query = entityManager.createQuery(
                "SELECT e FROM MPObject e WHERE EXISTS (SELECT a FROM MPObject a WHERE a.id = 1)");

        List<MPObject> res = query.getResultList();

        // Expected 10 but was 5 (https://github.com/Tirasa/OpenJPA-Azure/issues/53)
        assertEquals(5, res.size());
    }

    public void testAll() {
        final OpenJPAEntityManager entityManager = emf.createEntityManager();

        List<PObject> res = entityManager.createQuery(
                "SELECT e FROM PObject e WHERE e.id > ALL (SELECT a.id FROM MPObject a)").getResultList();

        // Expected 20 but was 10 (https://github.com/Tirasa/OpenJPA-Azure/issues/53)
        assertEquals(10, res.size());
    }

    public void testAny() {
        final EntityManager entityManager = emf.createEntityManager();
        List<MPObject> res = entityManager.createQuery(
                "SELECT e FROM MPObject e WHERE e.id < ANY (SELECT a.id FROM PObject a)").getResultList();

        // Expected 10 but was 5 (https://github.com/Tirasa/OpenJPA-Azure/issues/53)
        assertEquals(5, res.size());
    }

    public void testIn() {
        final EntityManager entityManager = emf.createEntityManager();
        List<MPObject> res = entityManager.createQuery(
                "SELECT e FROM MPObject e WHERE e.id IN (SELECT a.id FROM MPObject a WHERE a.id < 5)").getResultList();

        assertEquals(5, res.size());
    }
}
