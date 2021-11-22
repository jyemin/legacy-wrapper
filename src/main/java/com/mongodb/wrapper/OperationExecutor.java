package com.mongodb.wrapper;

import java.util.function.Supplier;

public interface OperationExecutor {
    <T> T execute(Supplier<T> operation);
    void beginExecution();
    void endExecution();

}
