# jdbc

Experimental JDBC wrapper for R. While still work in progress in the medium term
it is intended
to implement the `DBI` interface as closely as possible and conform to most
tests in `DBItest` where feasible.

## Basic usage
```
library("jdbc")
drv = jdbc::jdbc("org.sqlite.JDBC", "sqlite-jdbc-3.23.1.jar")
conn = dbConnect(drv, "jdbc:sqlite::memory:")

data("iris")
dbCreateTable(conn, "iris", iris)

stmt = dbSendStatement(conn, "INSERT INTO iris VALUES (?, ?, ?, ?, ?)")
dbBind(stmt, iris)

dbReadTable(conn, "iris")
```

...


