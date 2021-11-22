package com.mongodb.wrapper;

import com.mongodb.DBObject;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;

import java.util.List;

public class BulkWriteRequestBuilder {
    private final com.mongodb.BulkWriteRequestBuilder wrapped;

    public BulkWriteRequestBuilder(final com.mongodb.BulkWriteRequestBuilder wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Returns the collation
     *
     * @return the collation
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return wrapped.getCollation();
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public BulkWriteRequestBuilder collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    /**
     * Adds a request to remove all documents in the collection that match the query with which this builder was created.
     */
    public void remove() {
        wrapped.remove();
    }

    /**
     * Adds a request to remove one document in the collection that matches the query with which this builder was created.
     */
    public void removeOne() {
        wrapped.removeOne();
    }

    /**
     * Adds a request to replace one document in the collection that matches the query with which this builder was created.
     *
     * @param document the replacement document, which must be structured just as a document you would insert.  It can not contain any
     *                 update operators.
     */
    public void replaceOne(final DBObject document) {
        wrapped.replaceOne(document);
    }

    /**
     * Adds a request to update all documents in the collection that match the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void update(final DBObject update) {
        wrapped.update(update);
    }

    /**
     * Adds a request to update one document in the collection that matches the query with which this builder was created.
     *
     * @param update the update criteria
     */
    public void updateOne(final DBObject update) {
        wrapped.updateOne(update);
    }

    /**
     * Specifies that the request being built should be an upsert.
     *
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     * @mongodb.driver.manual tutorial/modify-documents/#upsert-option Upsert
     */
    public BulkUpdateRequestBuilder upsert() {
        return new BulkUpdateRequestBuilder(wrapped.upsert());
    }

    /**
     * Specifies that the request being built should use the given array filters for an update.  Note that this option only applies to
     * update operations and will be ignored for replace operations
     *
     * @param arrayFilters the array filters to apply to the update operation
     * @return a new builder that allows only update and replace, since upsert does not apply to remove.
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BulkUpdateRequestBuilder arrayFilters(final List<? extends DBObject> arrayFilters) {
        return new BulkUpdateRequestBuilder(wrapped.arrayFilters(arrayFilters));
    }
}
