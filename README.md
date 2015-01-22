# Cross Query

Query across multiple mysql hosts from a single HTTP endpoint.

## POST /query

### Request

Request must have a body similar to the following:

```{"AuthKey": "secret", "Sql": "SELECT foo FROM bar where foo=:foo", "Params": {"foo": "somevalue"}, "HostMatch": ".*"}```

| Key       | Required | Description
|-----------|----------|--------------------------------------------------------------------------------
| AuthKey   | YES      | Valid keys are defined in server config
| Sql       | YES      | SQL statement to execute
| Params    | NO       | If Sql contains named placeholders their values can included in a params dict
| HostMatch | NO       | Filter mysql hosts using a Regxp ([RE2](https://code.google.com/p/re2/wiki/Syntax))


### Response
If a request is malformed the response will return instantly with an error and error status code.

Assuming the request looks okay all hosts will be queried in parallel and rows will be streamed back in
no particular order (CSV format). If any host generates an error this will be buffered until all other rows have
been output then appended to the output.

Columns are given headings by the first result returned. Columns are not checked for consistency after this so it is up
to the user to ensure all hosts return the same number of columns in the same order (e.g. avoiding `SELECT *` if there
is a chance of a host schema mismatch)

sample output:
```
host,foo
localhost1,bar
localhost2,bar

DATA ENDS! ERRORS FOLLOW:
localhost3,Error 1045: Access denied for user 'root'@'localhost' (using password: NO)
```
