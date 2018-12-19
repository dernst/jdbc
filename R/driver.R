#' Driver for JDBC databases.
#'
#' @keywords internal
#' @export
#' @import DBI
#' @import methods
setClass("jdbcDriver",
         contains="DBIDriver",
         slots=list(jdrv="jobjRef"),
         prototype=list(jdrv=.jnull()))

#' @export
#' @rdname jdbc-class
# TODO: do we need to implement this?
setMethod("dbUnloadDriver", "jdbcDriver", function(drv, ...) {
    TRUE
})

setMethod("show", "jdbcDriver", function(object) {
    cat("<jdbcDriver>\n")
})

#' @export
jdbc <- function(driverClass=NULL, cp=NULL) {
    if(!is.null(cp))
        .jaddClassPath(cp)

    if(!is.null(driverClass)) {
        jdrv = .jnew(driverClass)
        ex = .jgetEx(TRUE)
        if(!is.jnull(ex)) {
            return(NULL)
        }
        new("jdbcDriver", jdrv=jdrv)
    } else {
        new("jdbcDriver", jdrv=.jnull())
    }
}

#' @export
setMethod("dbDataType", "jdbcDriver", function(dbObj, obj, ...) {
    #TODO: implement this properly
    if (is.factor(obj)) return("TEXT")
    if (is.data.frame(obj)) return(callNextMethod(dbObj, obj))
    #if (is.integer64(obj)) return("INTEGER")

    switch(typeof(obj),
           integer = "INTEGER",
           double = "REAL",
           character = "TEXT",
           logical = "INTEGER",
           list = "BLOB",
           raw = "TEXT",
           stop("Unsupported type", call. = FALSE)
           )
})

#' @rdname dbGetInfo
#' @export
# TODO: ...
setMethod("dbGetInfo", "jdbcDriver", function(dbObj, ...) {
    #warning("dbGetInfo is deprecated")
    list(driver.version = NA, client.version = NA)
})

#' @param drv An object created by \code{jdbc}
#' @rdname jdbc
#' @export
#' @examples
#' \dontrun{
#' db <- dbConnect(RKazam::Kazam())
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "SELECT * FROM mtcars WHERE cyl == 4")
#' }
setMethod("dbConnect", "jdbcDriver", function(drv, url='', user='', password='', ...) {
    jc <- .jcall("java/sql/DriverManager",
                 "Ljava/sql/Connection;",
                 "getConnection",
                 as.character(url)[1],
                 as.character(user)[1],
                 as.character(password)[1],
                 check=FALSE)

    ex = .jgetEx(TRUE)
    if(!is.jnull(ex)) {
        msg = .jcall(ex, "S", "getMessage")
        stop(paste0("connect error:\n", msg))
    }

    new("jdbcConnection", jconn=jc)
})


