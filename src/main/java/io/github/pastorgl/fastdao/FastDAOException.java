package io.github.pastorgl.fastdao;

public class FastDAOException extends RuntimeException {
    public FastDAOException(Exception e) {
        super(e);
    }

    public FastDAOException(String cause, Exception e) {
        super(cause, e);
    }
}
