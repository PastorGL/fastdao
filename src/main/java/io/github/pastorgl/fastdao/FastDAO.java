package io.github.pastorgl.fastdao;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.*;
import java.util.*;

/**
 * Abstract low-level DAO designed for bulk/batch operations.
 * Implementations must specify concrete {@link FastEntity} subclass as type parameter.
 *
 * @param <E> {@link FastEntity} subclass
 */
public abstract class FastDAO<E extends FastEntity> {
    static private int batchSize = 500;
    static private DataSource ds;
    /**
     * Physical name of Primary Key column
     */
    private String pkName;
    /**
     * Physical table name
     */
    private String tableName;
    /**
     * {@link FastEntity} subclass
     */
    private Class<E> persistentClass;
    /**
     * Persistent class field names to physical column names mapping
     */
    private Map<String, String> fwMapping = new HashMap<>();
    /**
     * Physical column names to persistent class field names mapping
     */
    private Map<String, String> revMapping = new HashMap<>();
    /**
     * Persistent class fields cache
     */
    private Map<String, Field> fields = new HashMap<>();
    /**
     * Persistent class field annotations cache
     */
    private Map<Field, Column> columns = new HashMap<>();

    {
        persistentClass = (Class<E>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];

        if (persistentClass.isAnnotationPresent(Table.class)) {
            tableName = persistentClass.getAnnotation(Table.class).value();
        } else {
            tableName = persistentClass.getSimpleName();
        }

        for (Field field : persistentClass.getDeclaredFields()) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                field.setAccessible(true);
                String fieldName = field.getName();

                fields.put(fieldName, field);

                String columnName;
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    columnName = column.value();
                    revMapping.put(columnName, fieldName);
                    fwMapping.put(fieldName, columnName);

                    columns.put(field, column);
                } else {
                    columnName = fieldName;
                }

                if (field.isAnnotationPresent(PK.class)) {
                    pkName = columnName;
                }
            }
        }

        if (pkName == null) {
            pkName = tableName + "_id";
        }
    }

    static public void setDataSource(DataSource ds) {
        FastDAO.ds = ds;
    }

    static protected DataSource getDataSource() {
        return ds;
    }

    static public void setBatchSize(int batchSize) {
        FastDAO.batchSize = batchSize;
    }

    /**
     * Call SELECT that returns a lizt of &lt;E&gt; instances
     *
     * @param query any SQL Query whose result is a list of &lt;E&gt;, optionally with ? for replaceable parameters.
     *              Use backslash to escape question marks
     * @param args  objects, whose values will be used as source of replaceable parameters. If object is an array or
     *              {@link List}, it'll be unfolded
     * @return list of &lt;E&gt;
     */
    protected List<E> select(String query, Object... args) {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            if (args.length != 0) {
                List<Object> expl = new ArrayList<>(args.length);

                StringBuilder sb = new StringBuilder();

                int r = 0;
                for (Object a : args) {
                    int q;
                    boolean found = false;
                    do {
                        q = query.indexOf('?', r);
                        if (q < 0) {
                            throw new IllegalArgumentException("supplied query and replaceable arguments don't match");
                        }
                        if ((q > 0) && (query.charAt(q - 1) == '\\')) {
                            r = q + 1;
                        } else {
                            found = true;
                        }
                    } while (!found);
                    sb.append(query, r, q);
                    r = q + 1;

                    if (a instanceof Object[]) {
                        a = Collections.singletonList(a);
                    }
                    if (a instanceof List) {
                        List<Object> aa = (List<Object>) a;
                        int s = aa.size();
                        sb.append('(');
                        for (int i = 0; i < s; i++) {
                            expl.add(aa.get(i));
                            if (i > 0) {
                                sb.append(',');
                            }
                            sb.append('?');
                        }
                        sb.append(')');
                    } else {
                        expl.add(a);
                        sb.append('?');
                    }
                }

                sb.append(query.substring(r));

                query = sb.toString();
                args = expl.toArray();
            }

            List<E> lst = new ArrayList<>();

            con = ds.getConnection();
            ps = con.prepareStatement(query);

            int c = 1;
            for (Object a : args) {
                setObject(ps, c++, a);
            }

            rs = ps.executeQuery();

            ResultSetMetaData md = rs.getMetaData();
            int cnt = md.getColumnCount();
            while (rs.next()) {
                E e = persistentClass.newInstance();

                for (int i = 1; i <= cnt; i++) {
                    String colName = md.getColumnLabel(i);
                    Field field = fields.get(getRevMapping(colName));
                    Class<?> type = field.getType();

                    if (type.isEnum()) {
                        field.set(e, Enum.valueOf((Class<Enum>) type, rs.getString(i)));
                    } else {
                        convertFromRetrieve(field, e, rs.getObject(i));
                    }
                }

                lst.add(e);
            }

            return lst;
        } catch (Exception e) {
            throw new FastDAOException("select", e);
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Batch insert of a list of &lt;E&gt; instances
     *
     * @param objects &lt;E&gt; instances
     */
    protected void insert(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        Connection con = null;
        PreparedStatement ps = null;

        try {
            StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");

            int k = 0;
            for (Field f : fields.values()) {
                String colName = getFwMapping(f.getName());
                if (!pkName.equals(colName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    sb.append(colName);
                }
            }

            sb.append(") VALUES ");
            int length = fields.size();
            int size = objects.size();

            sb.append("(");
            for (int j = 1; j < length; j++) {
                if (j > 1) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(")");

            con = ds.getConnection();
            ps = con.prepareStatement(sb.toString());
            int b = 0;
            for (int i = 0; i < size; i++, b++) {
                k = 1;
                Object o = objects.get(i);
                for (Field field : fields.values()) {
                    if (!pkName.equals(getFwMapping(field.getName()))) {
                        setObject(ps, k++, convertToStore(field, o));
                    }
                }
                ps.addBatch();

                if (b == batchSize) {
                    ps.executeBatch();

                    ps.clearBatch();
                    b = 0;
                }
            }
            if (b != 0) {
                ps.executeBatch();
            }
        } catch (Exception e) {
            throw new FastDAOException("insert - batch", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Insert one &lt;E&gt; instance
     *
     * @param object &lt;E&gt; instance
     * @return new object primary key value
     */
    protected Object insert(E object) {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");

            boolean generateKey = true;

            int k = 0, fc = 0;
            for (Field field : fields.values()) {
                String colName = getFwMapping(field.getName());
                if (!pkName.equals(colName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    sb.append(colName);
                    fc++;
                } else {
                    if (field.get(object) != null) {
                        if (k++ > 0) {
                            sb.append(",");
                        }
                        sb.append(colName);
                        fc++;
                        generateKey = false;
                    }
                }
            }

            sb.append(") VALUES (");
            for (int j = 0; j < fc; j++) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(")");

            con = ds.getConnection();
            ps = con.prepareStatement(sb.toString(), PreparedStatement.RETURN_GENERATED_KEYS);
            k = 1;
            Object key = null;
            Field keyField = null;
            for (Field field : fields.values()) {
                if (!pkName.equals(getFwMapping(field.getName()))) {
                    setObject(ps, k++, convertToStore(field, object));
                } else {
                    keyField = field;
                    if (!generateKey) {
                        key = convertToStore(keyField, object);
                        setObject(ps, k++, key);
                    }
                }
            }

            ps.executeUpdate();
            if (generateKey) {
                rs = ps.getGeneratedKeys();
                rs.next();
                switch (rs.getMetaData().getColumnCount()) {
                    case 0: // null key
                        break;
                    case 1: {
                        key = rs.getObject(1);
                        break;
                    }
                    default:
                        key = rs.getObject(pkName);
                }
            }

            return convertFromRetrieve(keyField, object, key);
        } catch (Exception e) {
            throw new FastDAOException("insert - single", e);
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Update a list of &lt;E&gt; instances matched by their primary key values
     *
     * @param objects &lt;E&gt; instances
     */
    protected void update(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        Connection con = null;

        PreparedStatement ps = null;

        try {
            StringBuilder sb = new StringBuilder("UPDATE " + tableName + " SET (");

            int length = fields.size();
            Field key = null;
            int k = 0;
            for (Field f : fields.values()) {
                String colName = getFwMapping(f.getName());
                if (!colName.equals(pkName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    sb.append(colName);
                } else {
                    key = f;
                }
            }

            sb.append(") = (");

            for (int j = 1; j < length; j++) {
                if (j > 1) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(") WHERE " + pkName + "=?");

            con = ds.getConnection();
            ps = con.prepareStatement(sb.toString());
            int b = 0;
            for (int i = 0; i < objects.size(); i++, b++) {
                Object object = objects.get(i);
                k = 1;
                for (Field field : fields.values()) {
                    if (!getFwMapping(field.getName()).equals(pkName)) {
                        setObject(ps, k++, convertToStore(field, object));
                    }
                }
                setObject(ps, k, convertToStore(key, object));
                ps.addBatch();

                if (b == batchSize) {
                    ps.executeBatch();
                    ps.clearBatch();
                    b = 0;
                }
            }
            if (b != 0) {
                ps.executeBatch();
            }
        } catch (Exception e) {
            throw new FastDAOException("update - batch", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Update single &lt;E&gt; instance matching by its primary key value
     *
     * @param object &lt;E&gt; instance
     */
    protected void update(E object) {
        Connection con = null;
        PreparedStatement ps = null;

        try {
            StringBuilder sb = new StringBuilder("UPDATE " + tableName + " SET (");

            int length = fields.size();
            Field key = null;
            int k = 0;
            for (Field f : fields.values()) {
                String colName = getFwMapping(f.getName());
                if (!colName.equals(pkName)) {
                    if (k++ > 0) {
                        sb.append(",");
                    }
                    sb.append(colName);
                } else {
                    key = f;
                }
            }

            sb.append(") = (");

            for (int j = 1; j < length; j++) {
                if (j > 1) {
                    sb.append(",");
                }
                sb.append("?");
            }
            sb.append(") WHERE " + pkName + "=?");

            con = ds.getConnection();
            ps = con.prepareStatement(sb.toString());
            k = 1;
            for (Field field : fields.values()) {
                if (!getFwMapping(field.getName()).equals(pkName)) {
                    setObject(ps, k++, convertToStore(field, object));
                }
            }
            setObject(ps, k, convertToStore(key, object));

            ps.executeUpdate();
        } catch (Exception e) {
            throw new FastDAOException("update - single", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Delete a list of &lt;E&gt; instances matching by their primary key values
     *
     * @param objects &lt;E&gt; instances
     */
    protected void delete(List<E> objects) {
        if (objects.size() == 0) {
            return;
        }

        Connection con = null;
        PreparedStatement ps = null;

        try {
            StringBuilder sb = new StringBuilder("DELETE FROM " + tableName + " WHERE " + pkName
                    + " IN (");

            int size = objects.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("?");
            }

            sb.append(")");

            Field key = fields.get(getRevMapping(pkName));

            con = ds.getConnection();
            ps = con.prepareStatement(sb.toString());
            int k = 1;
            for (E object : objects) {
                setObject(ps, k++, convertToStore(key, object));
            }

            ps.executeUpdate();
        } catch (Exception e) {
            throw new FastDAOException("delete - list", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Delete a single &lt;E&gt; instance matching by its primary key value
     *
     * @param object &lt;E&gt; instance
     */
    protected void delete(E object) {
        if (object == null) {
            return;
        }

        Connection con = null;
        PreparedStatement ps = null;

        try {
            Field key = fields.get(getRevMapping(pkName));

            con = ds.getConnection();
            ps = con.prepareStatement("DELETE FROM " + tableName + " WHERE " + pkName + "=?");
            setObject(ps, 1, convertToStore(key, object));

            ps.executeUpdate();
        } catch (Exception e) {
            throw new FastDAOException("delete - single", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    /**
     * Convenience method to get all &lt;E&gt; instances from the table
     *
     * @return all &lt;E&gt; instances
     */
    protected List<E> getAll() {
        return select("SELECT * FROM " + tableName);
    }

    /**
     * Get a single &lt;E&gt; instance matching by its primary key value
     *
     * @param pk primary key value
     * @return &lt;E&gt; instance
     */
    protected E getByPK(Object pk) {
        List<E> objects = select("SELECT * FROM " + tableName + " WHERE " + pkName + "=?", pk);

        if (objects.size() != 1) {
            return null;
        }

        return objects.get(0);
    }

    /**
     * Delete a single &lt;E&gt; instance matching by its primary key value
     *
     * @param pk primary key value
     */
    protected void deleteByPK(Object pk) {
        if (pk == null) {
            throw new FastDAOException("delete - single", new NullPointerException());
        }

        Class<?> type = fields.get(getRevMapping(pkName)).getType();
        if (!type.isInstance(pk)) {
            throw new FastDAOException("delete - single", new IllegalArgumentException(
                    "Unexpected primary key type. Expected: " + type.getCanonicalName() + " but passed is: " + pk.getClass()
                            .getCanonicalName()));
        }

        Connection con = null;
        PreparedStatement ps = null;

        try {

            con = ds.getConnection();
            ps = con.prepareStatement("DELETE FROM " + tableName + " WHERE " + pkName + "=?");
            setObject(ps, 1, pk);

            ps.executeUpdate();
        } catch (Exception e) {
            throw new FastDAOException("delete - single", e);
        } finally {
            closeStatement(ps);
            closeConnection(con);
        }
    }

    private String getRevMapping(String columnName) {
        if (revMapping.containsKey(columnName)) {
            return revMapping.get(columnName);
        }

        return columnName;
    }

    private String getFwMapping(String fieldName) {
        if (fwMapping.containsKey(fieldName)) {
            return fwMapping.get(fieldName);
        }

        return fieldName;
    }

    private void setObject(PreparedStatement s, int i, Object a) throws SQLException {
        if (a instanceof FastEntity) {
            s.setObject(i, ((FastEntity) a).getId());
            return;
        }

        if (a instanceof java.util.Date) {
            s.setDate(i, new java.sql.Date(((java.util.Date) a).getTime()));
            return;
        }

        if (a instanceof Enum) {
            s.setString(i, ((Enum<?>) a).name());
            return;
        }

        s.setObject(i, a);
    }

    private Object convertToStore(Field field, Object object) throws Exception {
        Object fieldValue = field.get(object);
        if (columns.containsKey(field)) {
            return columns.get(field).store().newInstance().store(ds.getConnection(), fieldValue);
        }

        return fieldValue;
    }

    private Object convertFromRetrieve(Field field, Object object, Object dbValue) throws Exception {
        Object value = dbValue;
        if (columns.containsKey(field)) {
            value = columns.get(field).retrieve().newInstance().retrieve(dbValue);
        }

        field.set(object, value);
        return value;
    }

    private void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                if (!stmt.isClosed()) {
                    stmt.close();
                }
            } catch (Exception e) {
                throw new FastDAOException("can't close Statement", e);
            }
        }
    }

    private void closeConnection(Connection con) {
        if (con != null) {
            try {
                if (!con.isClosed()) {
                    con.close();
                }
            } catch (Exception e) {
                throw new FastDAOException("can't close Connection", e);
            }
        }
    }

    private void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                if (!rs.isClosed()) {
                    rs.close();
                }
            } catch (Exception e) {
                throw new FastDAOException("can't close ResultSet", e);
            }
        }
    }
}
