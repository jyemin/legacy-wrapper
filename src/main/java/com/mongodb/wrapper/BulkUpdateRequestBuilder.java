/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.wrapper;

import com.mongodb.DBObject;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;

import java.util.List;

public class BulkUpdateRequestBuilder {
    private final com.mongodb.BulkUpdateRequestBuilder wrapped;

    public BulkUpdateRequestBuilder(final com.mongodb.BulkUpdateRequestBuilder wrapped) {
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
    public BulkUpdateRequestBuilder collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    /**
     * Gets the array filters to apply to the update operation
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    public List<? extends DBObject> getArrayFilters() {
        return wrapped.getArrayFilters();
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
}
