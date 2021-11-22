package com.mongodb.wrapper;

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;

import java.util.function.Supplier;

public class Cursor implements com.mongodb.Cursor {
    private final com.mongodb.Cursor wrapped;
    private final OperationExecutor operationExecutor;

    public Cursor(final com.mongodb.Cursor wrapped, final OperationExecutor operationExecutor) {
        this.wrapped = wrapped;
        this.operationExecutor = operationExecutor;
    }

    private <T> T wrap(Supplier<T> supplier) {
        return operationExecutor.execute(supplier);
    }

    @Override
    public long getCursorId() {
        return wrapped.getCursorId();
    }

    @Override
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public boolean hasNext() {
        if (available() > 0) {
            return wrapped.hasNext();
        } else {
            return wrap(wrapped::hasNext);
        }
    }

    @Override
    public DBObject next() {
        if (available() > 0) {
            return wrapped.next();
        } else {
            return wrap(wrapped::next);
        }
    }

    private int available() {
        // TODO: implement this
        throw new UnsupportedOperationException();
    }
}
