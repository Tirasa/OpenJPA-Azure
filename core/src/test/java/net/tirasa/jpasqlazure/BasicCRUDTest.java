/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.jpasqlazure;

import static org.junit.Assert.*;
import java.io.UnsupportedEncodingException;
import net.tirasa.jpasqlazure.beans.Gender;
import net.tirasa.jpasqlazure.beans.Person;
import net.tirasa.jpasqlazure.repository.PersonRepository;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
    "classpath:applicationContext.xml"
})
public class BasicCRUDTest {

    @Autowired
    private PersonRepository repository;

    @Test
    public void run()
            throws UnsupportedEncodingException {

        Person user = new Person();
        user.setUsername("Bob");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        user = repository.save(user);
        assertNotNull(user);
        assertEquals("picture", new String(user.getPicture(), "UTF-8"));

        user = repository.findOne(user.getId());
        assertNotNull(user);

        user.setInfo("other info");
        user = repository.save(user);
        assertNotNull(user);
        assertEquals("other info", user.getInfo());

        Iterable<Person> iter = repository.findAll();
        assertTrue(iter.iterator().hasNext());

        for (Person person : iter) {
            assertNotNull(person);
        }

        repository.delete(user);

        Person found = repository.findOne(user.getId());
        assertNull(found);
    }

    @Test
    @Ignore
    public void count() {
        assertTrue(repository.count() > 0);
    }
}
