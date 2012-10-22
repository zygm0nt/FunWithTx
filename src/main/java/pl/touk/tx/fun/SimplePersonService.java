package pl.touk.tx.fun;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;
import pl.touk.tx.fun.model.Person;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author mcl
 */
public class SimplePersonService {

    SimpleDao dao;

    @Transactional
    public void persist(Person p) {
        dao.update("insert into persons (name, age) values (?, ?)", p.toDbArray());
    }

    @Transactional
    public void persistWithException(Person p) {
        dao.update("insert into persons (name, age) values (?, ?)", p.toDbArray());
        throw new RuntimeException("intentional exception");
    }

    public List<Person> fetch() {
        return dao.query("select * from persons", new RowMapper<Person>() {
            @Override
            public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new Person(rs.getString("name"), rs.getInt("age"));
            }
        });
    }
    
    @Required
    public void setDao(SimpleDao dao) {
        this.dao = dao;
    }
}
