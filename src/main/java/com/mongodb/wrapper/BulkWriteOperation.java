package com.mongodb.wrapper;

import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.lang.Nullable;

import java.util.function.Supplier;

/**
 * A bulk write operation.  A bulk write operation consists of an ordered or unordered collection of write requests,
 * which can be any combination of inserts, updates, replaces, or removes.
 *
 * @see com.mongodb.DBCollection#initializeOrderedBulkOperation()
 * @see com.mongodb.DBCollection#initializeUnorderedBulkOperation()
 *
 * @mongodb.driver.manual /reference/command/delete/ Delete
 * @mongodb.driver.manual /reference/command/update/ Update
 * @mongodb.driver.manual /reference/command/insert/ Insert
 * @since 2.12
 */
public class BulkWriteOperation {
    private final com.mongodb.BulkWriteOperation wrapped;
    private final OperationExecutor operationExecutor;

    public BulkWriteOperation(final com.mongodb.BulkWriteOperation wrapped, final OperationExecutor operationExecutor) {
        this.wrapped = wrapped;
        this.operationExecutor = operationExecutor;
    }

    private <T> T wrap(Supplier<T> supplier) {
        return operationExecutor.execute(supplier);
    }

    /**
     * Returns true if this is building an ordered bulk write request.
     *
     * @return whether this is building an ordered bulk write operation
     * @see com.mongodb.DBCollection#initializeOrderedBulkOperation()
     * @see DBCollection#initializeUnorderedBulkOperation()
     */
    public boolean isOrdered() {
        return wrapped.isOrdered();
    }

    /**
     * Gets whether to bypass document validation, or null if unspecified.  The default is null.
     *
     * @return whether to bypass document validation, or null if unspecified.
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    @Nullable
    public Boolean getBypassDocumentValidation() {
        return wrapped.getBypassDocumentValidation();
    }

    /**
     * Sets whether to bypass document validation.
     *
     * @param bypassDocumentValidation whether to bypass document validation, or null if unspecified
     * @since 2.14
     * @mongodb.server.release 3.2
     */
    public void setBypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        wrapped.setBypassDocumentValidation(bypassDocumentValidation);
    }

    /**
     * Add an insert request to the bulk operation
     *
     * @param document the document to insert
     */
    public void insert(final DBObject document) {
        wrapped.insert(document);
    }

    /**
     * Start building a write request to add to the bulk write operation.  The returned builder can be used to create an update, replace,
     * or remove request with the given query.
     *
     * @param query the query for an update, replace or remove request
     * @return a builder for a single write request
     */
    public BulkWriteRequestBuilder find(final DBObject query) {
        return new BulkWriteRequestBuilder(wrapped.find(query));
    }

    /**
     * Execute the bulk write operation with the default write concern of the collection from which this came.  Note that the
     * continueOnError property of the write concern is ignored.
     *
     * @return the result of the bulk write operation.
     * @throws com.mongodb.BulkWriteException if the write failed due some other failure specific to the write command
     * @throws MongoException if the operation failed for some other reason
     */
    public BulkWriteResult execute() {
        return wrap(wrapped::execute);
    }

    /**
     * Execute the bulk write operation with the given write concern.  Note that the continueOnError property of the write concern is
     * ignored.
     *
     * @param writeConcern the write concern to apply to the bulk operation.
     * @return the result of the bulk write operation.
     * @throws com.mongodb.BulkWriteException if the write failed due some other failure specific to the write command
     * @throws MongoException if the operation failed for some other reason
     */
    public BulkWriteResult execute(final WriteConcern writeConcern) {
        return wrap(() -> wrapped.execute(writeConcern));
    }
}
