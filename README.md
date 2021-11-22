# legacy-wrapper
Wrapper over the mongo-java-driver legacy API

This approach looks feasible to implement some sort of bulkheading pattern over the legacy API (and could be extended to the non-legacy CRUD API)

The one thing missing is the ability to only enter the bulkhead while iterating a cursor when iteration is going to do I/O
(i.e. to execute a getMore command).  But I think we can handle that case if we implement https://jira.mongodb.org/browse/JAVA-4167 in the DBCursor
API.  Using a check for `available() > 0` before entering the bulkhead should be sufficient.
