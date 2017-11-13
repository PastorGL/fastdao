package io.github.pastorgl.fastdao;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertTrue;

public class FastDAOTest {

    @BeforeClass
    public static void setup() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false");
        FastDAO.setDataSource(ds);

        ds.getConnection().createStatement().execute("CREATE TABLE test_entity (id int8 auto_increment primary key, varchar varchar, bool boolean, enum varchar, list array)");

        FastDAO.setDataSource(ds);
    }

    @Test
    public void operationsTest() {
        TestDAO underTest = new TestDAO();

        TestEntity one = new TestEntity();
        one.setBool(true);
        one.setEnum(TestEnum.C);
        one.setId(21L);
        one.setList(Arrays.asList(3, 4, 5));
        one.setVarchar("string");

        Long id = (Long)underTest.insert(one);

        TestEntity _one = underTest.getByPK(id);

        one.setId(id);
        assertTrue(one.equals(_one));

    }

    @Table("test_entity")
    public static class TestEntity extends FastEntity {
        @PK
        private Long id;

        @Column("varchar")
        private String varchar;

        @Column("enum")
        private TestEnum _enum;

        @Column("bool")
        private Boolean bool;

        @Column(value = "list", store = TestConverter.class, retrieve = TestConverter.class)
        private List<Integer> list;

        @Override
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public List<Integer> getList() {
            return list;
        }

        public void setList(List<Integer> list) {
            this.list = list;
        }

        public String getVarchar() {
            return varchar;
        }

        public void setVarchar(String varchar) {
            this.varchar = varchar;
        }

        public TestEnum getEnum() {
            return _enum;
        }

        public void setEnum(TestEnum _enum) {
            this._enum = _enum;
        }

        public Boolean getBool() {
            return bool;
        }

        public void setBool(Boolean bool) {
            this.bool = bool;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestEntity)) return false;
            TestEntity that = (TestEntity) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(varchar, that.varchar) &&
                    _enum == that._enum &&
                    Objects.equals(bool, that.bool) &&
                    Objects.equals(list, that.list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, varchar, _enum, bool, list);
        }
    }

    public static class TestDAO extends FastDAO<TestEntity> {
        @Override
        public List<TestEntity> select(String query, Object... args) {
            return super.select(query, args);
        }

        @Override
        public void insert(List<TestEntity> objects) {
            super.insert(objects);
        }

        @Override
        public Object insert(TestEntity object) {
            return super.insert(object);
        }

        @Override
        public void update(List<TestEntity> objects) {
            super.update(objects);
        }

        @Override
        public void update(TestEntity object) {
            super.update(object);
        }

        @Override
        public void delete(List<TestEntity> objects) {
            super.delete(objects);
        }

        @Override
        public void delete(TestEntity object) {
            super.delete(object);
        }

        @Override
        public List<TestEntity> getAll() {
            return super.getAll();
        }

        @Override
        public TestEntity getByPK(Object pk) {
            return super.getByPK(pk);
        }

        @Override
        public void deleteByPK(Object pk) {
            super.deleteByPK(pk);
        }
    }

    public enum TestEnum {
        A,
        B,
        C;
    }

    public static class TestConverter implements StoreConverter, RetrieveConverter {
        @Override
        public Object retrieve(Object dbValue) throws SQLException {
            return Arrays.asList((Object[])dbValue);
        }

        @Override
        public Object store(Connection connection, Object fieldValue) throws SQLException {
            return connection.createArrayOf("integer", ((List) fieldValue).toArray());
        }
    }
}
