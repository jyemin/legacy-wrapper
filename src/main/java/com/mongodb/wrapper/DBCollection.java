package com.mongodb.wrapper;

import com.mongodb.AggregationOptions;
import com.mongodb.CommandResult;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoder;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteResult;
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.client.model.DBCollectionDistinctOptions;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;
import com.mongodb.client.model.DBCollectionFindOptions;
import com.mongodb.client.model.DBCollectionRemoveOptions;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DBCollection {
    private final com.mongodb.DBCollection wrapped;
    private final OperationExecutor operationExecutor;

    public DBCollection(final com.mongodb.DBCollection wrapped, final OperationExecutor operationExecutor) {
        this.wrapped = wrapped;
        this.operationExecutor = operationExecutor;
    }

    private <T> T wrap(Supplier<T> supplier) {
       return operationExecutor.execute(supplier);
    }

    /**
     * Insert a document into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param document     {@code DBObject} to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject document, final WriteConcern writeConcern) {
        return wrap(() -> wrapped.insert(document, writeConcern));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added. Collection wide {@code WriteConcern} will be used.
     *
     * @param documents {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject... documents) {
        return wrap(() -> wrapped.insert(documents));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException        if the write failed due some other failure
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final WriteConcern writeConcern, final DBObject... documents) {
        return wrap(() -> wrapped.insert(writeConcern, documents));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents    {@code DBObject}'s to be inserted
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject[] documents, final WriteConcern writeConcern) {
        return wrap(() -> wrapped.insert(documents, writeConcern));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents list of {@code DBObject} to be inserted
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents) {
        return wrap(() -> wrapped.insert(documents));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final WriteConcern aWriteConcern) {
        return wrap(() -> wrapped.insert(documents,aWriteConcern));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param encoder       {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final DBObject[] documents, final WriteConcern aWriteConcern, final DBEncoder encoder) {
        return wrap(() -> wrapped.insert(documents, aWriteConcern, encoder));
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param documents     a list of {@code DBObject}'s to be inserted
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @param dbEncoder     {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final WriteConcern aWriteConcern,
                              @Nullable final DBEncoder dbEncoder) {
        return wrap(() -> wrapped.insert(documents, aWriteConcern, dbEncoder));

    }

    /**
     * <p>Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.</p>
     *
     * <p>If the value of the continueOnError property of the given {@code InsertOptions} is true,
     * that value will override the value of the continueOnError property of the given {@code WriteConcern}. Otherwise,
     * the value of the continueOnError property of the given {@code WriteConcern} will take effect. </p>
     *
     * @param documents     a list of {@code DBObject}'s to be inserted
     * @param insertOptions the options to use for the insert
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/insert-documents/ Insert Documents
     */
    public WriteResult insert(final List<? extends DBObject> documents, final InsertOptions insertOptions) {
        return wrap(() -> wrapped.insert(documents, insertOptions));
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectId value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     *     <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in
     *     the document.</li>
     *     <li>If a document exists with the specified '_id' value, the method performs an update,
     *     replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document {@link DBObject} to save to the collection.
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert or update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save(final DBObject document) {
        return wrap(() -> wrapped.save(document));
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectId value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field:
     * <ul>
     *     <li>If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in
     *     the document.</li>
     *     <li>If a document exists with the specified '_id' value, the method performs an update,
     *     replacing all field in the existing record with the fields from the document.</li>
     * </ul>
     *
     * @param document     {@link DBObject} to save to the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the insert or update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    public WriteResult save(final DBObject document, final WriteConcern writeConcern) {
        return wrap(() -> wrapped.save(document, writeConcern));
    }

    /**
     * Modify an existing document or documents in collection. The query parameter employs the same query selectors, as used in {@code
     * find()}.
     *
     * @param query         the selection criteria for the update
     * @param update        the modifications to apply
     * @param upsert        when true, inserts a document if no document matches the update query criteria
     * @param multi         when true, updates all documents in the collection that match the update query criteria, otherwise only updates
     *                      one
     * @param aWriteConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern aWriteConcern) {
        return wrap(() -> wrapped.update(query, update, upsert, multi, aWriteConcern));
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@code find()}.
     *
     * @param query   the selection criteria for the update
     * @param update  the modifications to apply
     * @param upsert  when true, inserts a document if no document matches the update query criteria
     * @param multi   when true, updates all documents in the collection that match the update query criteria, otherwise only updates
     *                one
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, @Nullable final DBEncoder encoder) {
        return wrap(() -> wrapped.update(query, update, upsert, multi, concern, encoder));
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link com.mongodb.DBCollection#find(DBObject)}.
     *
     * @param query                    the selection criteria for the update
     * @param update                   the modifications to apply
     * @param upsert                   when true, inserts a document if no document matches the update query criteria
     * @param multi                    when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @param concern                  {@code WriteConcern} to be used during operation
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param encoder                  the DBEncoder to use
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     * @since 2.14
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi,
                              final WriteConcern concern, @Nullable final Boolean bypassDocumentValidation,
                              @Nullable final DBEncoder encoder) {
        return wrap(() -> wrapped.update(query, update, upsert, multi, concern, bypassDocumentValidation, encoder));
    }

    /**
     * Modify an existing document or documents in collection. The query parameter employs the same query selectors, as used in {@code
     * find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @param upsert when true, inserts a document if no document matches the update query criteria
     * @param multi  when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update, final boolean upsert, final boolean multi) {
        return wrap(() -> wrapped.update(query, update, upsert, multi));
    }

    /**
     * Modify an existing document. The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult update(final DBObject query, final DBObject update) {
        return wrap(() -> wrapped.update(query, update));
    }

    /**
     * Modify documents in collection. The query parameter employs the same query selectors, as used in {@code find()}.
     *
     * @param query  the selection criteria for the update
     * @param update the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify Documents
     */
    public WriteResult updateMulti(final DBObject query, final DBObject update) {
        return wrap(() -> wrapped.update(query, update));
    }

    /**
     * Modify an existing document or documents in collection.
     *
     * @param query   the selection criteria for the update
     * @param update  the modifications to apply
     * @param options the options to apply to the update operation
     * @return the result of the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     * @since 3.4
     */
    public WriteResult update(final DBObject query, final DBObject update, final DBCollectionUpdateOptions options) {
        return wrap(() -> wrapped.update(query, update, options));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents
     *              in the collection.
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query) {
        return wrap(() -> wrapped.remove(query));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern) {
        return wrap(() -> wrapped.remove(query, writeConcern));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query        the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                     documents in the collection.
     * @param writeConcern {@code WriteConcern} to be used during operation
     * @param encoder      {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     */
    public WriteResult remove(final DBObject query, final WriteConcern writeConcern, final DBEncoder encoder) {
        return wrap(() -> wrapped.remove(query, writeConcern, encoder));
    }

    /**
     * Remove documents from a collection.
     *
     * @param query   the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                documents in the collection.
     * @param options the options to apply to the delete operation
     * @return the result of the operation
     * @throws com.mongodb.WriteConcernException if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual tutorial/remove-documents/ Remove Documents
     * @since 3.4
     */
    public WriteResult remove(final DBObject query, final DBCollectionRemoveOptions options) {
        return wrap(() -> wrapped.remove(query, options));
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query the selection criteria using query operators. Omit the query parameter or pass an empty document to return all documents
     *              in the collection.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find(final DBObject query) {
        return new DBCursor(wrapped.find(query), this, operationExecutor);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query      the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                   documents in the collection.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find(final DBObject query, final DBObject projection) {
        return new DBCursor(wrapped.find(query, projection), this, operationExecutor);
    }

    /**
     * Select all documents in collection and get a cursor to the selected documents.
     *
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    public DBCursor find() {
        return new DBCursor(wrapped.find(), this, operationExecutor);
    }

    /**
     * Select documents in collection and get a cursor to the selected documents.
     *
     * @param query   the selection criteria using query operators. Omit the query parameter or pass an empty document to return all
     *                documents in the collection.
     * @param options the options for the find operation.
     * @return A cursor to the documents that match the query criteria
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @since 3.4
     */
    public DBCursor find(@Nullable final DBObject query, final DBCollectionFindOptions options) {
        return new DBCursor(wrapped.find(query, options), this, operationExecutor);
    }

    /**
     * Get a single document from collection.
     *
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne() {
        return wrap(wrapped::findOne);
    }

    /**
     * Get a single document from collection.
     *
     * @param query the selection criteria using query operators.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query) {
        return wrap(() -> wrapped.findOne(query));
    }

    /**
     * Get a single document from collection.
     *
     * @param query      the selection criteria using query operators.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection) {
        return wrap(() -> wrapped.findOne(query, projection));
    }

    /**
     * Get a single document from collection.
     *
     * @param query      the selection criteria using query operators.
     * @param projection specifies which fields MongoDB will return from the documents in the result set.
     * @param sort       A document whose fields specify the attributes on which to sort the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection, final DBObject sort) {
        return wrap(() -> wrapped.findOne(query, projection, sort));
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param projection     specifies which fields MongoDB will return from the documents in the result set.
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final DBObject query, final DBObject projection, final ReadPreference readPreference) {
        return wrap(() -> wrapped.findOne(query, projection, readPreference));
    }

    /**
     * Get a single document from collection.
     *
     * @param query          the selection criteria using query operators.
     * @param projection     specifies which projection MongoDB will return from the documents in the result set.
     * @param sort           A document whose fields specify the attributes on which to sort the result set.
     * @param readPreference {@code ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(@Nullable final DBObject query, @Nullable final DBObject projection, @Nullable final DBObject sort,
                            final ReadPreference readPreference) {
        return wrap(() -> wrapped.findOne(query, projection, sort, readPreference));
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id value of '_id' field of a document we are looking for
     * @return A document with '_id' provided as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final Object id) {
        return wrap(() -> wrapped.findOne(id));
    }

    /**
     * Get a single document from collection by '_id'.
     *
     * @param id         value of '_id' field of a document we are looking for
     * @param projection specifies which projection MongoDB will return from the documents in the result set.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     */
    @Nullable
    public DBObject findOne(final Object id, final DBObject projection) {
        return wrap(() -> wrapped.findOne(id, projection));
    }

    /**
     * Get a single document from collection.
     *
     * @param query       the selection criteria using query operators.
     * @param findOptions the options for the find operation.
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Querying
     * @since 3.4
     */
    @Nullable
    public DBObject findOne(@Nullable final DBObject query, final DBCollectionFindOptions findOptions) {
        return wrap(() -> wrapped.findOne(query, findOptions));
    }

    /**
     * Same as {@link #getCount()}
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count() {
        return wrap(wrapped::count);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(@Nullable final DBObject query) {
        return wrap(() -> wrapped.count(query));
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query          specifies the selection criteria
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long count(@Nullable final DBObject query, final ReadPreference readPreference) {
        return wrap(() -> wrapped.count(query, readPreference));
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query   specifies the selection criteria
     * @param options the options for the count operation.
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     * @since 3.4
     */
    public long count(@Nullable final DBObject query, final DBCollectionCountOptions options) {
        return wrap(() -> wrapped.count(query, options));
    }

    /**
     * Get the count of documents in collection.
     *
     * @return the number of documents in collection
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount() {
        return wrap(wrapped::getCount);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     */
    public long getCount(@Nullable final DBObject query) {
        return wrap(() -> wrapped.getCount(query));
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query   specifies the selection criteria
     * @param options the options for the count operation.
     * @return the number of documents that matches selection criteria
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/count/ Count
     * @since 3.4
     */
    public long getCount(@Nullable final DBObject query, final DBCollectionCountOptions options) {
        return wrap(() -> wrapped.getCount(query, options));
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName specifies the new name of the collection
     * @return the collection with new name
     * @throws MongoException if newName is the name of an existing collection.
     * @mongodb.driver.manual reference/command/renameCollection/ Rename Collection
     */
    public DBCollection rename(final String newName) {
        return new DBCollection(wrap(() -> wrapped.rename(newName)), operationExecutor);
    }

    /**
     * Change the name of an existing collection.
     *
     * @param newName    specifies the new name of the collection
     * @param dropTarget If {@code true}, mongod will drop the collection with the target name if it exists
     * @return the collection with new name
     * @throws MongoException if target is the name of an existing collection and {@code dropTarget=false}.
     * @mongodb.driver.manual reference/command/renameCollection/ Rename Collection
     */
    public DBCollection rename(final String newName, final boolean dropTarget) {
        return new DBCollection(wrap(() -> wrapped.rename(newName, dropTarget)), operationExecutor);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values.
     * @return a List of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName) {
        return wrap(() -> wrapped.distinct(fieldName));
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return a List of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final ReadPreference readPreference) {
        return wrap(() -> wrapped.distinct(fieldName, readPreference));
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values
     * @param query     specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return an array of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final DBObject query) {
        return wrap(() -> wrapped.distinct(fieldName, query));
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName      Specifies the field for which to return the distinct values
     * @param query          specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    public List distinct(final String fieldName, final DBObject query, final ReadPreference readPreference) {
        return wrap(() -> wrapped.distinct(fieldName, query, readPreference));
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param fieldName Specifies the field for which to return the distinct values
     * @param options   the options to apply for this operation
     * @return A {@code List} of the distinct values
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     * @since 3.4
     */
    public List distinct(final String fieldName, final DBCollectionDistinctOptions options) {
        return wrap(() -> wrapped.distinct(fieldName, options));
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final DBObject query) {
        return wrap(() -> wrapped.mapReduce(map, reduce, outputTarget, query));
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation.
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query) {
        return wrap(() -> wrapped.mapReduce(map, outputTarget, outputTarget, query));
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param map            a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce         a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget   specifies the location of the result of the map-reduce operation.
     * @param outputType     specifies the type of job output
     * @param query          specifies the selection criteria using query operators for determining the documents input to the map
     *                       function.
     * @param readPreference the read preference specifying where to run the query.  Only applied for Inline output type
     * @return a MapReduceOutput which contains the results of this map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final String map, final String reduce, final String outputTarget,
                                     final MapReduceCommand.OutputType outputType, final DBObject query,
                                     final ReadPreference readPreference) {
        return wrap(() -> wrapped.mapReduce(map, reduce, outputTarget, outputType, query, readPreference));
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.
     *
     * @param command specifies the details of the Map Reduce operation to perform
     * @return a MapReduceOutput containing the results of the map reduce operation
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    public MapReduceOutput mapReduce(final MapReduceCommand command) {
        return wrap(() -> wrapped.mapReduce(command));
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @param options  options to apply to the aggregation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    public Cursor aggregate(final List<? extends DBObject> pipeline, final AggregationOptions options) {
        return wrap(() -> new Cursor(wrapped.aggregate(pipeline, options), operationExecutor));
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline       operations to be performed in the aggregation pipeline
     * @param options        options to apply to the aggregation
     * @param readPreference {@link ReadPreference} to be used for this operation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    public Cursor aggregate(final List<? extends DBObject> pipeline, final AggregationOptions options,
                            final ReadPreference readPreference) {
        return wrap(() -> new Cursor(wrapped.aggregate(pipeline, options, readPreference), operationExecutor));
    }

    /**
     * Return the explain plan for the aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline to explain
     * @param options  the options to apply to the aggregation
     * @return the command result.  The explain output may change from release to release, so best to simply log this.
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.driver.manual reference/operator/meta/explain/ Explain query
     * @mongodb.server.release 3.6
     */
    public CommandResult explainAggregate(final List<? extends DBObject> pipeline, final AggregationOptions options) {
        return wrap(() -> wrapped.explainAggregate(pipeline, options));
    }

    /**
     * Get the name of a collection.
     *
     * @return the name of a collection
     */
    public String getName() {
        return wrapped.getName();
    }

    /**
     * Get the full name of a collection, with the database name as a prefix.
     *
     * @return the name of a collection
     * @mongodb.driver.manual reference/glossary/#term-namespace Namespace
     */
    public String getFullName() {
        return wrapped.getFullName();
    }

    /**
     * Find a collection that is prefixed with this collection's name. A typical use of this might be
     * <pre>{@code
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * }</pre>
     * Which is equivalent to
     * <pre>{@code
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * }</pre>
     *
     * @param name the name of the collection to find
     * @return the matching collection
     */
    public DBCollection getCollection(final String name) {
        return new DBCollection(wrapped.getCollection(name), operationExecutor);
    }

    /**
     * Forces creation of an ascending index on a field with the default options.
     *
     * @param name name of field to index on
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final String name) {
        wrap(() -> {
            wrapped.createIndex(name);
            return null;
        });

    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name an identifier for the index. If null or empty, the default name will be used.
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, final String name) {
        wrap(() -> {
            wrapped.createIndex(keys, name);
            return null;
        });
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys   a document that contains pairs with the name of the field or fields to index and order of the index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, @Nullable final String name, final boolean unique) {
        wrap(() -> {
            wrapped.createIndex(keys, name, unique);
            return null;
        });

    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys) {
        wrap(() -> {
            wrapped.createIndex(keys);
            return null;
        });
    }

    /**
     * Creates an index on the field specified, if that index does not already exist.
     *
     * <p>Prior to MongoDB 3.0 the dropDups option could be used with unique indexes allowing documents with duplicate values to be dropped
     * when building the index. Later versions of MongoDB will silently ignore this setting. </p>
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    public void createIndex(final DBObject keys, final DBObject options) {
        wrap(() -> {
            wrapped.createIndex(keys, options);
            return null;
        });
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param sort   determines which document the operation will modify if the query selects multiple documents
     * @param update the modifications to apply
     * @return pre-modification document
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject sort, final DBObject update) {
        return wrap(() -> wrapped.findAndModify(query, sort, update));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query  specifies the selection criteria for the modification
     * @param update the modifications to apply
     * @return the document as it was before the modifications
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, final DBObject update) {
        return wrap(() -> wrapped.findAndModify(query, update));
    }

    /**
     * Atomically remove and return a single document. The returned document is the original document before removal.
     *
     * @param query specifies the selection criteria for the modification
     * @return the document as it was before the modifications
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndRemove(@Nullable final DBObject query) {
        return wrap(() -> wrapped.findAndRemove(query));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query     specifies the selection criteria for the modification
     * @param fields    a subset of fields to return
     * @param sort      determines which document the operation will modify if the query selects multiple documents
     * @param remove    when {@code true}, removes the selected document
     * @param returnNew when true, returns the modified document rather than the original
     * @param update    the modifications to apply
     * @param upsert    when true, operation creates a new document if the query returns no documents
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query        specifies the selection criteria for the modification
     * @param fields       a subset of fields to return
     * @param sort         determines which document the operation will modify if the query selects multiple documents
     * @param remove       when true, removes the selected document
     * @param returnNew    when true, returns the modified document rather than the original
     * @param update       the modifications to apply
     * @param upsert       when true, operation creates a new document if the query returns no documents
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, final DBObject update, final boolean returnNew,
                                  final boolean upsert, final WriteConcern writeConcern) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert, writeConcern));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when true, removes the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param update      the modifications to apply
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it.
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.12.0
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
                maxTime, maxTimeUnit));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query        specifies the selection criteria for the modification
     * @param fields       a subset of fields to return
     * @param sort         determines which document the operation will modify if the query selects multiple documents
     * @param remove       when {@code true}, removes the selected document
     * @param returnNew    when true, returns the modified document rather than the original
     * @param update       performs an update of the selected document
     * @param upsert       when true, operation creates a new document if the query returns no documents
     * @param maxTime      the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit  the unit that maxTime is specified in
     * @param writeConcern the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    @Nullable
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
                maxTime, maxTimeUnit, writeConcern));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query                    specifies the selection criteria for the modification
     * @param fields                   a subset of fields to return
     * @param sort                     determines which document the operation will modify if the query selects multiple documents
     * @param remove                   when {@code true}, removes the selected document
     * @param returnNew                when true, returns the modified document rather than the original
     * @param update                   performs an update of the selected document
     * @param upsert                   when true, operation creates a new document if the query returns no documents
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param maxTime                  the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit              the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    @Nullable
    public DBObject findAndModify(final DBObject query, final DBObject fields, final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
                bypassDocumentValidation, maxTime, maxTimeUnit));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query                    specifies the selection criteria for the modification
     * @param fields                   a subset of fields to return
     * @param sort                     determines which document the operation will modify if the query selects multiple documents
     * @param remove                   when {@code true}, removes the selected document
     * @param returnNew                when true, returns the modified document rather than the original
     * @param update                   performs an update of the selected document
     * @param upsert                   when true, operation creates a new document if the query returns no documents
     * @param bypassDocumentValidation whether to bypass document validation.
     * @param maxTime                  the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit              the unit that maxTime is specified in
     * @param writeConcern             the write concern to apply to this operation
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.14.0
     */
    public DBObject findAndModify(@Nullable final DBObject query, @Nullable final DBObject fields, @Nullable final DBObject sort,
                                  final boolean remove, @Nullable final DBObject update,
                                  final boolean returnNew, final boolean upsert,
                                  final boolean bypassDocumentValidation,
                                  final long maxTime, final TimeUnit maxTimeUnit,
                                  final WriteConcern writeConcern) {
        return wrap(() -> wrapped.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
                bypassDocumentValidation, maxTime, maxTimeUnit, writeConcern));
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query   specifies the selection criteria for the modification
     * @param options the options regarding the find and modify operation
     * @return the document as it was before the modifications, unless {@code oprtions.returnNew} is true, in which case it returns the
     * document after the changes were made
     * @throws WriteConcernException             if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed for some other reason
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 3.4
     */
    public DBObject findAndModify(final DBObject query, final DBCollectionFindAndModifyOptions options) {
        return wrap(() -> wrapped.findAndModify(query, options));
    }

    /**
     * Returns the database this collection is a member of.
     *
     * @return this collection's database
     * @mongodb.driver.manual reference/glossary/#term-database Database
     */
    public DB getDB() {
        return new DB(wrapped.getDB(), operationExecutor);
    }

    /**
     * Get the {@link WriteConcern} for this collection.
     *
     * @return the default write concern for this collection
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    /**
     * Set the {@link WriteConcern} for this collection. Will be used for writes to this collection. Overrides any setting of write concern
     * at the DB level.
     *
     * @param writeConcern WriteConcern to use
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        wrapped.setWriteConcern(writeConcern);
    }

    /**
     * Gets the {@link ReadPreference}.
     *
     * @return the default read preference for this collection
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    /**
     * Sets the {@link ReadPreference} for this collection. Will be used as default for reads from this collection; overrides DB and
     * Connection level settings. See the documentation for {@link ReadPreference} for more information.
     *
     * @param preference ReadPreference to use
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public void setReadPreference(final ReadPreference preference) {
        wrapped.setReadPreference(getReadPreference());
    }

    /**
     * Sets the read concern for this collection.
     *
     * @param readConcern the read concern to use for this collection
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     * @since 3.3
     */
    public void setReadConcern(final ReadConcern readConcern) {
        wrapped.setReadConcern(readConcern);
    }

    /**
     * Get the read concern for this collection.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     * @since 3.3
     */
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    /**
     * Drops (deletes) this collection from the database. Use with care.
     *
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws MongoException                    if the operation failed
     * @mongodb.driver.manual reference/command/drop/ Drop Command
     */
    public void drop() {
        wrap(() -> {
            wrapped.drop();
            return null;
        });
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoClientOptions is being used.
     *
     * @return the factory
     */
    public DBDecoderFactory getDBDecoderFactory() {
        return wrapped.getDBDecoderFactory();
    }

    /**
     * Set a custom decoder factory for this collection.  Set to null to use the default from MongoClientOptions.
     *
     * @param factory the factory to set.
     */
    public void setDBDecoderFactory(@Nullable final DBDecoderFactory factory) {
        wrapped.setDBDecoderFactory(factory);
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoClientOptions is being used.
     *
     * @return the factory
     */
    public DBEncoderFactory getDBEncoderFactory() {
        return wrapped.getDBEncoderFactory();
    }

    /**
     * Set a custom encoder factory for this collection.  Set to null to use the default from MongoClientOptions.
     *
     * @param factory the factory to set.
     */
    public void setDBEncoderFactory(@Nullable final DBEncoderFactory factory) {
        wrapped.setDBEncoderFactory(factory);
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public List<DBObject> getIndexInfo() {
        return wrap(wrapped::getIndexInfo);
    }

    /**
     * Drops an index from this collection.  The DBObject index parameter must match the specification of the index to drop, i.e. correct
     * key name and type must be specified.
     *
     * @param index the specification of the index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndex(final DBObject index) {
        wrap(() -> {
            wrapped.dropIndex(index);
            return null;
        });
    }

    /**
     * Drops the index with the given name from this collection.
     *
     * @param indexName name of index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndex(final String indexName) {
        wrap(() -> {
            wrapped.dropIndex(indexName);
            return null;
        });
    }

    /**
     * Drop all indexes on this collection.  The default index on the _id field will not be deleted.
     *
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndexes() {
        wrap(() -> {
            wrapped.dropIndexes();
            return null;
        });
    }

    /**
     * Drops the index with the given name from this collection.  This method is exactly the same as {@code dropIndex(indexName)}.
     *
     * @param indexName name of index to drop
     * @throws MongoException if the index does not exist
     * @mongodb.driver.manual core/indexes/ Indexes
     */
    public void dropIndexes(final String indexName) {
        wrap(() -> {
            wrapped.dropIndexes(indexName);
            return null;
        });
    }

    /**
     * The collStats command returns a variety of storage statistics for a given collection
     *
     * @return a CommandResult containing the statistics about this collection
     * @mongodb.driver.manual reference/command/collStats/ collStats Command
     */
    public CommandResult getStats() {
        return wrap(wrapped::getStats);
    }

    /**
     * Checks whether this collection is capped
     *
     * @return true if this is a capped collection
     * @mongodb.driver.manual /core/capped-collections/#check-if-a-collection-is-capped Capped Collections
     */
    public boolean isCapped() {
        return wrap(wrapped::isCapped);
    }

    /**
     * Gets the default class for objects in the collection
     *
     * @return the class
     */
    public Class getObjectClass() {
        return wrapped.getObjectClass();
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     *
     * @param aClass the class
     */
    public void setObjectClass(final Class<? extends DBObject> aClass) {
        wrapped.setObjectClass(aClass);
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path   the path to map the given Class to
     * @param aClass the Class to map the given path to
     */
    public void setInternalClass(final String path, final Class<? extends DBObject> aClass) {
        wrapped.setInternalClass(path, aClass);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

    /**
     * <p>Creates a builder for an ordered bulk write operation, consisting of an ordered collection of write requests, which can be any
     * combination of inserts, updates, replaces, or removes. Write requests included in the bulk operations will be executed in order, and
     * will halt on the first failure.</p>
     *
     * <p>Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.</p>
     *
     * <p>Note: While a bulk write operation with a mix of inserts, updates, replaces, and removes is supported, the implementation will
     * batch up consecutive requests of the same type and send them to the server one at a time.  For example, if a bulk write operation
     * consists of 10 inserts followed by 5 updates, followed by 10 more inserts, it will result in three round trips to the server.</p>
     *
     * @return the builder
     * @mongodb.driver.manual reference/method/db.collection.initializeOrderedBulkOp/ initializeOrderedBulkOp()
     * @since 2.12
     */
    public BulkWriteOperation initializeOrderedBulkOperation() {
        return new BulkWriteOperation(wrapped.initializeOrderedBulkOperation(), operationExecutor);
    }

    /**
     * <p>Creates a builder for an unordered bulk operation, consisting of an unordered collection of write requests, which can be any
     * combination of inserts, updates, replaces, or removes. Write requests included in the bulk operation will be executed in an undefined
     * order, and all requests will be executed even if some fail.</p>
     *
     * <p>Note: While this bulk write operation will execute on MongoDB 2.4 servers and below, the writes will be performed one at a time,
     * as that is the only way to preserve the semantics of the value returned from execution or the exception thrown.</p>
     *             
     * @return the builder
     * @mongodb.driver.manual reference/method/db.collection.initializeUnorderedBulkOp/ initializeUnorderedBulkOp()
     * @since 2.12
     */
    public BulkWriteOperation initializeUnorderedBulkOperation() {
        return new BulkWriteOperation(wrapped.initializeUnorderedBulkOperation(), operationExecutor);
    }
}
