# fastdao
Reflection-based strongly-typeing object wrapper on top of JDBC primarily aimed for batch/bulk operations. Core java, no external dependencies.

# Example

Entity (baked by DB table) definition:

```java
import io.github.pastorgl.fastdao.Column;
import io.github.pastorgl.fastdao.FastEntity;
import io.github.pastorgl.fastdao.PK;
import io.github.pastorgl.fastdao.Table;

import java.util.Objects;

@Table(TestPojo.TABLE_NAME)
public class TestPojo extends FastEntity {
    public static final String TABLE_NAME = "test_pojo";
    public static final String NAME_NAME = "test_varchar";
    public static final String FLAG_NAME = "test_bool";
    public static final String ENUM_NAME = "tet_enum";

    @PK
    private Long id;

    @Column(NAME_NAME)
    private String name;

    @Column(FLAG_NAME)
    private Boolean flag;

    @Column(ENUM_NAME)
    private TestEnum testEnum;

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getFlag() {
        return flag;
    }

    public void setFlag(Boolean flag) {
        this.flag = flag;
    }

    public TestEnum getTestEnum() {
        return testEnum;
    }

    public void setTestEnum(TestEnum testEnum) {
        this.testEnum = testEnum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestPojo)) return false;
        TestPojo testPojo = (TestPojo) o;
        return Objects.equals(id, testPojo.id) &&
                Objects.equals(name, testPojo.name) &&
                Objects.equals(flag, testPojo.flag) &&
                testEnum == testPojo.testEnum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, flag, testEnum);
    }
}

public enum TestEnum {
    ONE,
    TWO,
    THREE;
}
```

Its DAO (to be injected in upper application layers) that makes necessary methods public:

```java
import io.github.pastorgl.fastdao.FastDAO;

import javax.inject.Singleton;
import java.util.List;

@Singleton
public class TestPojoDAO extends FastDAO<TestPojo> {
    @Override
    public List<TestPojo> select(String query, Object... args) {
        return super.select(query, args);
    }

    @Override
    public Object insert(TestPojo object) {
        return super.insert(object);
    }

    @Override
    public void update(TestPojo object) {
        super.update(object);
    }

    @Override
    public List<TestPojo> getAll() {
        return super.getAll();
    }

    public TestPojo getByPK(Long pk) {
        return super.getByPK(pk);
    }

    public void deleteByPK(Long pk) {
        super.deleteByPK(pk);
    }
}
```

# Advanced example

Non-DB-baked (DTO type, virtual) entity:

```java
import io.github.pastorgl.fastdao.Column;
import io.github.pastorgl.fastdao.FastEntity;

public class EntityCount extends FastEntity {
    @Column("count")
    private Long id;

    @Override
    public Long getId() {
        return id;
    }
}
```

And its corresponding DAO that has a method to return count of rows for any DB-baked FastEntity descendant:

```java
import io.github.pastorgl.fastdao.FastDAO;
import io.github.pastorgl.fastdao.FastEntity;
import io.github.pastorgl.fastdao.Table;

import javax.inject.Singleton;

@Singleton
public class EntityCountDAO extends FastDAO<EntityCount> {
    public Long getCount(Class<? extends FastEntity> entityClass) {
        return select("SELECT COUNT(*) AS count FROM " + entityClass.getAnnotation(Table.class).value()).get(0).getId();
    }
}
```
