package com.mongodb.wrapper;

import com.mongodb.Cursor;
import com.mongodb.CursorType;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * <p>An iterator over database results. Doing a {@code find()} query on a collection returns a {@code DBCursor}.</p>
 * <p> An application should ensure that a cursor is closed in all circumstances, e.g. using a try-with-resources statement:</p>
 * <blockquote><pre>
 *    try (DBCursor cursor = collection.find(query)) {
 *        while (cursor.hasNext()) {
 *            System.out.println(cursor.next();
 *        }
 *    }
 * </pre></blockquote>
 *
 * <p><b>Warning:</b> Calling {@code toArray} or {@code length} on a DBCursor will irrevocably turn it into an array.  This means that, if
 * the cursor was iterating over ten million results (which it was lazily fetching from the database), suddenly there will be a ten-million
 * element array in memory.  Before converting to an array, make sure that there are a reasonable number of results using {@code skip()} and
 * {@code limit()}.
 *
 * <p>For example, to get an array of the 1000-1100th elements of a cursor, use</p>
 *
 * <pre>{@code
 *    List<DBObject> obj = collection.find(query).skip(1000).limit(100).toArray();
 * }</pre>
 *
 * @mongodb.driver.manual core/read-operations Read Operations
 */
@NotThreadSafe
public class DBCursor implements Cursor, Iterable<DBObject> {

    private final com.mongodb.DBCursor wrapped;
    private final DBCollection collection;
    private final OperationExecutor operationExecutor;

    DBCursor(final com.mongodb.DBCursor wrapped, DBCollection collection, final OperationExecutor operationExecutor) {
        this.wrapped = wrapped;
        this.collection = collection;
        this.operationExecutor = operationExecutor;
    }

    private <T> T wrap(Supplier<T> supplier) {
        return operationExecutor.execute(supplier);
    }

    /**
     * Creates a copy of an existing database cursor. The new cursor is an iterator, even if the original was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        return new DBCursor(wrapped.copy(), collection, operationExecutor);
    }

    /**
     * Checks if there is another object available.
     *
     * <p><em>Note</em>: Automatically turns cursors of type Tailable to TailableAwait. For non blocking tailable cursors see
     * {@link #tryNext }.</p>
     *
     * @return true if there is another object available
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Override
    public boolean hasNext() {
        if (available() > 0) {
            return wrapped.hasNext();
        } else {
            return wrap(wrapped::hasNext);
        }
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     *
     * <p><em>Note</em>: Automatically turns cursors of type Tailable to TailableAwait. For non blocking tailable cursors see
     * {@link #tryNext }.</p>
     *
     * @return the next element
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Override
    public DBObject next() {
        if (available() > 0) {
            return wrapped.next();
        } else {
            return wrap(wrapped::next);
        }
    }

    /**
     * Non blocking check for tailable cursors to see if another object is available.
     *
     * <p>Returns the object the cursor is at and moves the cursor ahead by one or
     * return null if no documents is available.</p>
     *
     * @return the next element or null
     * @throws MongoException if failed
     * @throws IllegalArgumentException if the cursor is not tailable
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Nullable
    public DBObject tryNext() {
        if (available() > 0) {
            return wrapped.tryNext();
        } else {
            return wrap(wrapped::tryNext);
        }
    }

    private int available() {
        // TODO: add this
        throw new UnsupportedOperationException();
    }


    /**
     * Returns the element the cursor is at.
     *
     * @return the current element
     */
    public DBObject curr() {
        return wrapped.curr();
    }

    @Override
    public void remove() {
        wrapped.remove();
    }

    /**
     * Gets the query limit.
     *
     * @return the limit, or 0 if no limit is set
     */
    public int getLimit() {
        return wrapped.getLimit();
    }

    /**
     * Gets the batch size.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    /**
     * Adds a comment to the query to identify queries in the database profiler output.
     *
     * @param comment the comment that is to appear in the profiler output
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/comment/ $comment
     * @since 2.12
     */
    public DBCursor comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    /**
     * Specifies an <em>exclusive</em> upper limit for the index to use in a query.
     *
     * @param max a document specifying the fields, and the upper bound values for those fields
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/max/ $max
     * @since 2.12
     */
    public DBCursor max(final DBObject max) {
        wrapped.max(max);
        return this;
    }

    /**
     * Specifies an <em>inclusive</em> lower limit for the index to use in a query.
     *
     * @param min a document specifying the fields, and the lower bound values for those fields
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/min/ $min
     * @since 2.12
     */
    public DBCursor min(final DBObject min) {
        wrapped.min(min);
        return this;
    }

    /**
     * Forces the cursor to only return fields included in the index.
     *
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/returnKey/ $returnKey
     * @since 2.12
     */
    public DBCursor returnKey() {
        wrapped.returnKey();
        return this;
    }

    /**
     * Informs the database of indexed fields of the collection in order to improve performance.
     *
     * @param indexKeys a {@code DBObject} with fields and direction
     * @return same DBCursor for chaining operations
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public DBCursor hint(final DBObject indexKeys) {
        wrapped.hint(indexKeys);
        return this;
    }

    /**
     * Set the maximum execution time for operations on this cursor.
     *
     * @param maxTime  the maximum time that the server will allow the query to run, before killing the operation.
     * @param timeUnit the time unit
     * @return same DBCursor for chaining operations
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ $maxTimeMS
     * @since 2.12.0
     */
    public DBCursor maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    /**
     * Returns an object containing basic information about the execution of the query that created this cursor. This creates a {@code
     * DBObject} with a number of fields, including but not limited to:
     * <ul>
     *     <li><i>cursor:</i> cursor type</li>
     *     <li><i>nScanned:</i> number of records examined by the database for this query </li>
     *     <li><i>n:</i> the number of records that the database returned</li>
     *     <li><i>millis:</i> how long it took the database to execute the query</li>
     * </ul>
     *
     * @return a {@code DBObject} containing the explain output for this DBCursor's query
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/explain Explain Output
     * @mongodb.server.release 3.0
     */
    @Deprecated
    public DBObject explain() {
        return wrap(wrapped::explain);
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type, which may not be null
     * @return this
     * @since 3.9
     */
    public DBCursor cursorType(final CursorType cursorType) {
        wrapped.cursorType(cursorType);
        return this;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     * @since 3.9
     * @deprecated oplogReplay has been deprecated in MongoDB 4.4.
     */
    @Deprecated
    public DBCursor oplogReplay(final boolean oplogReplay) {
        wrapped.oplogReplay(oplogReplay);
        return this;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use. Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     * @since 3.9
     */
    public DBCursor noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     * @since 3.9
     */
    public DBCursor partial(final boolean partial) {
        wrapped.partial(partial);
        return this;
    }

    /**
     * Sorts this cursor's elements. This method must be called before getting any object from the cursor.
     *
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort(final DBObject orderBy) {
        wrapped.sort(orderBy);
        return this;
    }

    /**
     * Limits the number of elements returned. Note: parameter {@code limit} should be positive, although a negative value is
     * supported for legacy reason. Passing a negative value will call {@link DBCursor#batchSize(int)} which is the preferred method.
     *
     * @param limit the number of elements to return
     * @return a cursor to iterate the results
     * @mongodb.driver.manual reference/method/cursor.limit Limit
     */
    public DBCursor limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    /**
     * <p>Limits the number of elements returned in one batch. A cursor typically fetches a batch of result objects and store them
     * locally.</p>
     *
     * <p>If {@code batchSize} is positive, it represents the size of each batch of objects retrieved. It can be adjusted to optimize
     * performance and limit data transfer.</p>
     *
     * <p>If {@code batchSize} is negative, it will limit of number objects returned, that fit within the max batch size limit (usually
     * 4MB), and cursor will be closed. For example if {@code batchSize} is -10, then the server will return a maximum of 10 documents and
     * as many as can fit in 4MB, then close the cursor. Note that this feature is different from limit() in that documents must fit within
     * a maximum size, and it removes the need to send a request to close the cursor server-side.</p>
     *
     * @param numberOfElements the number of elements to return in a batch
     * @return {@code this} so calls can be chained
     */
    public DBCursor batchSize(final int numberOfElements) {
        wrapped.batchSize(numberOfElements);
        return this;
    }

    /**
     * Discards a given number of elements at the beginning of the cursor.
     *
     * @param numberOfElements the number of elements to skip
     * @return a cursor pointing to the new first element of the results
     * @throws IllegalStateException if the cursor has started to be iterated through
     */
    public DBCursor skip(final int numberOfElements) {
        wrapped.skip(numberOfElements);
        return this;
    }

    @Override
    public long getCursorId() {
        return wrapped.getCursorId();
    }

    /**
     * Returns the number of objects through which the cursor has iterated.
     *
     * @return the number of objects seen
     */
    public int numSeen() {
        return wrapped.numSeen();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    /**
     * <p>Creates a copy of this cursor object that can be iterated. Note: - you can iterate the DBCursor itself without calling this method
     * - no actual data is getting copied.</p>
     *
     * <p>Note that use of this method does not let you close the underlying cursor in the case of either an exception or an early
     * break.  The preferred method of iteration is to use DBCursor as an Iterator, so that you can call close() on it in a finally
     * block.</p>
     *
     * @return an iterator
     */
    @Override
    public Iterator<DBObject> iterator() {
        return wrapped.iterator();
    }

    /**
     * Converts this cursor to an array.
     *
     * @return an array of elements
     * @throws MongoException if failed
     */
    public List<DBObject> toArray() {
        return wrap(wrapped::toArray);
    }

    /**
     * Converts this cursor to an array.
     *
     * @param max the maximum number of objects to return
     * @return an array of objects
     * @throws MongoException if failed
     */
    public List<DBObject> toArray(final int max) {
        return wrap(() -> wrapped.toArray(max));
    }

    /**
     * Counts the number of objects matching the query. This does not take limit/skip into consideration, and does initiate a call to the
     * server.
     *
     * @return the number of objects
     * @throws MongoException if the operation failed
     * @see DBCursor#size
     */
    public int count() {
        return wrap(wrapped::count);
    }

    /**
     * Returns the first document that matches the query.
     *
     * @return the first matching document
     * @since 2.12
     */
    @Nullable
    public DBObject one() {
        return wrap(wrapped::one);
    }

    /**
     * Pulls back all items into an array and returns the number of objects. Note: this can be resource intensive.
     *
     * @return the number of elements in the array
     * @throws MongoException if failed
     * @see #count()
     * @see #size()
     */
    public int length() {
        return wrap(wrapped::length);
    }

    /**
     * For testing only! Iterates cursor and counts objects
     *
     * @return num objects
     * @throws MongoException if failed
     * @see #count()
     */
    public int itcount() {
        return wrap(wrapped::itcount);
    }

    /**
     * Counts the number of objects matching the query this does take limit/skip into consideration
     *
     * @return the number of objects
     * @throws MongoException if the operation failed
     * @see #count()
     */
    public int size() {
        return wrap(wrapped::size);
    }

    /**
     * Gets the fields to be returned.
     *
     * @return the field selector that cursor used
     */
    @Nullable
    public DBObject getKeysWanted() {
        return wrapped.getKeysWanted();
    }

    /**
     * Gets the query.
     *
     * @return the query that cursor used
     */
    public DBObject getQuery() {
        return wrapped.getQuery();
    }

    /**
     * Gets the collection.
     *
     * @return the collection that data is pulled from
     */
    public DBCollection getCollection() {
        return collection;
    }

    @Override
    @Nullable
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    /**
     * Sets the read preference for this cursor. See the documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference read preference to use
     * @return {@code this} so calls can be chained
     */
    public DBCursor setReadPreference(final ReadPreference readPreference) {
        wrapped.setReadPreference(readPreference);
        return this;
    }

    /**
     * Gets the default read preference.
     *
     * @return the readPreference used by this cursor
     */
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return wrapped.getCollation();
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public DBCursor setCollation(@Nullable final Collation collation) {
        wrapped.setCollation(collation);
        return this;
    }

    /**
     * Sets the factory that will be used create a {@code DBDecoder} that will be used to decode BSON documents into DBObject instances.
     *
     * @param factory the DBDecoderFactory
     * @return {@code this} so calls can be chained
     */
    public DBCursor setDecoderFactory(final DBDecoderFactory factory) {
        wrapped.setDecoderFactory(factory);
        return this;
    }

    /**
     * Gets the decoder factory that creates the decoder this cursor will use to decode objects from MongoDB.
     *
     * @return the decoder factory.
     */
    public DBDecoderFactory getDecoderFactory() {
        return wrapped.getDecoderFactory();
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }
}
