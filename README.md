# jdbc

Experimental JDBC wrapper for R

## Basic usage
```
library("jdbc")
drv = jdbc::jdbc("org.sqlite.JDBC", "sqlite-jdbc-3.23.1.jar")
conn = dbConnect(drv, "jdbc:sqlite::memory:")
```



