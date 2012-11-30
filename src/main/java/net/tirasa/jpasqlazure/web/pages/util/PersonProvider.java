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
package net.tirasa.jpasqlazure.web.pages.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.tirasa.jpasqlazure.beans.Person;
import net.tirasa.jpasqlazure.persistence.PersonDAO;
import org.apache.wicket.extensions.markup.html.repeater.data.sort.SortOrder;
import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;

public class PersonProvider extends SortableDataProvider<Person> {

    private final PersonDAO dao;

    public PersonProvider(final PersonDAO dao) {
        this.dao = dao;
        setSort("id", SortOrder.ASCENDING);
    }

    @Override
    public Iterator<? extends Person> iterator(int first, int count) {
        final List<Person> persons = new ArrayList<Person>(size());
        for (Person person : dao.findAll()) {
            persons.add(person);
        }

        return persons.subList(first, first + count).iterator();
    }

    @Override
    public int size() {
        Iterable<Person> all = dao.findAll();

        int count = 0;

        for (Person person : all) {
            count++;
        }
        return count;
    }

    @Override
    public IModel<Person> model(final Person person) {
        return new LoadableDetachableModel<Person>(person) {

            @Override
            protected Person load() {
                return dao.find(person.getId());
            }
        };
    }
}
