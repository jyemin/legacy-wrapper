package com.mongodb.wrapper;

import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Bulkhead implements OperationExecutor {
    private final Semaphore semaphore;
    private final long maxWaitTimeMS;
    private final int maxWaitQueueSize;
    private final AtomicInteger waitQueueSize = new AtomicInteger();

    public Bulkhead(int permits, long maxWaitTimeMS, int maxWaitQueueSize) {
        this.semaphore = new Semaphore(permits, true);
        this.maxWaitTimeMS = maxWaitTimeMS;
        this.maxWaitQueueSize = maxWaitQueueSize;
    }

    @Override
    public <T> T execute(Supplier<T> operation) {
        if (waitQueueSize.incrementAndGet() > maxWaitQueueSize) {
            waitQueueSize.decrementAndGet();
            throw new MongoWaitQueueFullException();
        }
        try {
            try {
                if (!semaphore.tryAcquire(maxWaitTimeMS, MILLISECONDS)) {
                    throw new MongoTimeoutException("blah blah");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MongoInterruptedException("blah blah", e);
            }
            try {
                return operation.get();
            } finally {
                semaphore.release();
            }
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void beginExecution() {
        if (waitQueueSize.incrementAndGet() > maxWaitQueueSize) {
            waitQueueSize.decrementAndGet();
            throw new MongoWaitQueueFullException();
        }
        try {
            if (!semaphore.tryAcquire(maxWaitTimeMS, MILLISECONDS)) {
                throw new MongoTimeoutException("blah blah");
            }
        } catch (InterruptedException e) {
            waitQueueSize.decrementAndGet();
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException("blah blah", e);
        }
    }

    @Override
    public void endExecution() {
        semaphore.release();
        waitQueueSize.decrementAndGet();
    }
}
