package io.github.pastorgl.fastdao;

import java.sql.Connection;
import java.sql.SQLException;

public interface StoreConverter {
    Object store(Connection connection, Object fieldValue) throws SQLException;

    class NullConverter implements StoreConverter {
        @Override
        public Object store(Connection connection, Object fieldValue) {
            return fieldValue;
        }
    }
}
