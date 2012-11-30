/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tirasa.jpasqlazure.persistence;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceContextType;
import net.tirasa.jpasqlazure.beans.Person;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Configurable
@Repository
@Transactional
public class PersonDAO {

    @Value("#{entityManager}")
    @PersistenceContext(type = PersistenceContextType.TRANSACTION)
    protected EntityManager entityManager;

    public Person find(final Long id) {
        return entityManager.find(Person.class, id);
    }

    public Person save(final Person person) {
        return entityManager.merge(person);
    }

    public void delete(final Person person) {
        Person p = find(person.getId());
        entityManager.remove(p);
    }

    public List<Person> findAll() {
        return entityManager.createQuery("SELECT e FROM Person e", Person.class).getResultList();
    }
}
