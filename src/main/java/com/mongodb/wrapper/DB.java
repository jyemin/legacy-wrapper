package com.mongodb.wrapper;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.client.model.DBCreateViewOptions;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class DB {
    private final com.mongodb.DB wrapped;
    private final OperationExecutor operationExecutor;

    public DB(final com.mongodb.DB wrapped, final OperationExecutor operationExecutor) {
        this.wrapped = wrapped;
        this.operationExecutor = operationExecutor;
    }

    private <T> T wrap(Supplier<T> supplier) {
        return operationExecutor.execute(supplier);
    }

    /**
     * Gets the MongoClient instance
     *
     * @return the MongoClient instance that this database was constructed from
     * @throws IllegalStateException if this DB was not created from a MongoClient instance
     * @since 3.9
     */
    public MongoClient getMongoClient() {
        return wrapped.getMongoClient();
    }

    /**
     * Sets the read preference for this database. Will be used as default for read operations from any collection in this database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference {@code ReadPreference} to use
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public void setReadPreference(final ReadPreference readPreference) {
        wrapped.setReadPreference(readPreference);
    }

    /**
     * Sets the write concern for this database. It will be used for write operations to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern {@code WriteConcern} to use
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        wrapped.setWriteConcern(writeConcern);
    }

    /**
     * Gets the read preference for this database.
     *
     * @return {@code ReadPreference} to be used for read operations, if not specified explicitly
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    /**
     * Gets the write concern for this database.
     *
     * @return {@code WriteConcern} to be used for write operations, if not specified explicitly
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    /**
     * Sets the read concern for this database.
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
     * Get the read concern for this database.
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
     * Gets a collection with a given name.
     *
     * @param name the name of the collection to return
     * @return the collection
     * @throws IllegalArgumentException if the name is invalid
     * @see MongoNamespace#checkCollectionNameValidity(String)
     */
    public DBCollection getCollection(final String name) {
        return new DBCollection(wrapped.getCollection(name), operationExecutor);
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     *
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/dropDatabase/ Drop Database
     */
    public void dropDatabase() {
        wrap(() -> {
            wrapped.dropDatabase();
            return null;
        });
    }

    /**
     * Returns the name of this database.
     *
     * @return the name
     */
    public String getName() {
        return wrapped.getName();
    }

    /**
     * Returns a set containing the names of all collections in this database.
     *
     * @return the names of collections in this database
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/method/db.getCollectionNames/ getCollectionNames()
     */
    public Set<String> getCollectionNames() {
        return wrap(wrapped::getCollectionNames);
    }

    /**
     * <p>Creates a collection with a given name and options. If the collection already exists,
     * this throws a {@code CommandFailureException}.</p>
     *
     * <p>Possible options:</p>
     * <ul>
     *     <li> <b>capped</b> ({@code boolean}) - Enables a collection cap. False by default. If enabled,
     *     you must specify a size parameter. </li>
     *     <li> <b>size</b> ({@code int}) - If capped is true, size specifies a maximum size in bytes for the capped collection. When
     *     capped is false, you may use size to preallocate space. </li>
     *     <li> <b>max</b> ({@code int}) -   Optional. Specifies a maximum "cap" in number of documents for capped collections. You must
     *     also specify size when specifying max. </li>
     * </ul>
     * <p>Note that if the {@code options} parameter is {@code null}, the creation will be deferred to when the collection is written
     * to.</p>
     *
     * @param collectionName the name of the collection to return
     * @param options        options
     * @return the collection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException        for all other failures
     * @mongodb.driver.manual reference/method/db.createCollection/ createCollection()
     */
    public DBCollection createCollection(final String collectionName, @Nullable final DBObject options) {
        return new DBCollection(wrap(() -> wrapped.createCollection(collectionName, options)), operationExecutor);
    }

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @return the view as a DBCollection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException        for all other failures
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.4
     */
    public DBCollection createView(final String viewName, final String viewOn, final List<? extends DBObject> pipeline) {
        return new DBCollection(wrap(() -> wrapped.createView(viewName, viewOn, pipeline)), operationExecutor);
    }

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param options  the options for creating the view
     * @return the view as a DBCollection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException        for all other failures
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.4
     */
    public DBCollection createView(final String viewName, final String viewOn, final List<? extends DBObject> pipeline,
                                   final DBCreateViewOptions options) {
        return new DBCollection(wrap(() -> wrapped.createView(viewName, viewOn, pipeline, options)), operationExecutor);
    }

    /**
     * Executes a database command. This method constructs a simple DBObject using {@code command} as the field name and {@code true} as its
     * value, and calls {@link com.mongodb.DB#command(DBObject, ReadPreference) } with the default read preference for the database.
     *
     * @param command command to execute
     * @return result of command from the database
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final String command) {
        return wrap(() -> wrapped.command(command));
    }

    /**
     * Executes a database command. This method calls {@link com.mongodb.DB#command(DBObject, ReadPreference) } with the default read preference for the
     * database.
     *
     * @param command {@code DBObject} representation of the command to be executed
     * @return result of the command execution
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject command) {
        return wrap(() -> wrapped.command(command));
    }

    /**
     * Executes a database command. This method calls {@link com.mongodb.DB#command(DBObject, ReadPreference, DBEncoder) } with the default read
     * preference for the database.
     *
     * @param command {@code DBObject} representation of the command to be executed
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject command, final DBEncoder encoder) {
        return wrap(() -> wrapped.command(command, encoder));
    }

    /**
     * Executes a database command with the selected readPreference, and encodes the command using the given encoder.
     *
     * @param command        The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @param encoder        The DBEncoder that knows how to serialise the command
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject command, final ReadPreference readPreference, @Nullable final DBEncoder encoder) {
        return wrap(() -> wrapped.command(command, readPreference, encoder));
    }

    /**
     * Executes the command against the database with the given read preference.
     *
     * @param command        The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject command, final ReadPreference readPreference) {
        return wrap(() -> wrapped.command(command, readPreference));
    }

    /**
     * Executes a database command. This method constructs a simple {@link DBObject} and calls {@link com.mongodb.DB#command(DBObject, ReadPreference)
     * }.
     *
     * @param command        The name of the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of the command execution
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final String command, final ReadPreference readPreference) {
        return command(new BasicDBObject(command, true), readPreference);
    }

    /**
     * Gets another database on same server
     *
     * @param name name of the database
     * @return the DB for the given name
     */
    public DB getSisterDB(final String name) {
        return new DB(wrapped.getSisterDB(name), operationExecutor);
    }

    /**
     * Checks to see if a collection with a given name exists on a server.
     *
     * @param collectionName a name of the collection to test for existence
     * @return {@code false} if no collection by that name exists, {@code true} if a match to an existing collection was found
     * @throws MongoException if the operation failed
     */
    public boolean collectionExists(final String collectionName) {
        Set<String> collectionNames = getCollectionNames();
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

}
