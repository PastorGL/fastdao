package io.github.pastorgl.fastdao;

import java.sql.SQLException;

public interface RetrieveConverter {
    Object retrieve(Object dbValue) throws SQLException;

    class NullConverter implements RetrieveConverter {
        @Override
        public Object retrieve(Object dbValue) {
            return dbValue;
        }
    }
}
