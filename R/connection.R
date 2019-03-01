#' JDBC connection class.
#' 
#' @export
#' @keywords internal
setClass("jdbcConnection", 
         contains = "DBIConnection", 
         slots = list(host = "character", 
                      username = "character", 
                      jconn = "jobjRef"),
         prototype = list(host='', username='', jconn=.jnull()))


#' @export
#' @rdname jdbc
setMethod("dbDisconnect", "jdbcConnection", function(conn, ...) {
    .jcall(conn@jconn, "V", "close")
    #TODO: handle sqlexception
    invisible(TRUE)
})

#' @export
format.jdbcConnection = function(x, ...) {
    "<jdbcConnection>"
}

#' Send a query to jdbc.
#' 
#' @export
# This is another good place to put examples
# TODO: support placeholders
setMethod("dbSendQuery", signature("jdbcConnection", "character"),
          function(conn, statement, ...) {
    stmt = .jcall(conn@jconn, "Ljava/sql/Statement;", "createStatement")
    rs = .jcall(stmt, "Ljava/sql/ResultSet;", "executeQuery", as.character(statement[1L]))
    # some code
    new("jdbcResult", jresult=rs)
})


#' Send a statement
#' @export
setMethod("dbSendStatement", signature("jdbcConnection", "character"), function(conn, statement, ...) {
    statement = as.character(statement)
    rs = .jcall(conn@jconn, "Ljava/sql/PreparedStatement;", "prepareStatement", statement)
    new("jdbcResult", jresult=rs, env=new.env(TRUE, emptyenv()))
})


#' @rdname hidden_aliases
#' @export
setMethod("dbExecute", signature("DBIConnection", "character"),
    function(conn, statement, param=list(), ...) {
        rs <- dbSendStatement(conn, statement, ...)
        if(length(param) > 0)
            dbBind(rs, params=param)
        on.exit(dbClearResult(rs))
        dbGetRowsAffected(rs)
    }
)

#' @exportMethod dbQuoteIdentifier
setMethod("dbQuoteIdentifier", c("jdbcConnection", "character"), function(conn, x, ...) {
    if(length(x) < 1L)
        return(SQL(character()))
    if(.jcall(conn@jconn, "Z", "isClosed"))
        stop("connection is closed")
    md = .jcall(conn@jconn, "Ljava/sql/DatabaseMetaData;", "getMetaData")
    qs = .jcall(md, "S", "getIdentifierQuoteString")
    SQL(paste0(qs, x, qs), names=names(x))
})

#' @exportMethod dbQuoteIdentifier
setMethod("dbQuoteIdentifier", c("jdbcConnection", "SQL"), function(conn, x, ...) {
    x
})


#' @exportMethod dbListTables
setMethod("dbListTables", c("jdbcConnection"), function(conn, schema=NULL, ...) {
    md = .jcall(conn@jconn, "Ljava/sql/DatabaseMetaData;", "getMetaData")
    if(is.null(schema)) schema = "%"
    ret = .jcall(md, "Ljava/sql/ResultSet;", "getTables",
                 .jnull("java/lang/String"), schema, "%",
                 .jnull("[Ljava/lang/String;"))
    rs = new("jdbcResult", jresult=ret)
    x = dbFetch(rs)
    x$TABLE_NAME
})

#' @exportMethod dbGetTables
#setMethod("dbGetTables", "jdbcConnection", function(conn, schema=NULL, ...) {
#    md = .jcall(conn@jconn, "Ljava/sql/DatabaseMetaData;", "getMetaData")
#    if(is.null(schema)) schema = "%"
#    ret = .jcall(md, "Ljava/sql/ResultSet;", "getTables",
#                 .jnull("java/lang/String"), schema, "%",
#                 .jnull("[Ljava/lang/String;"))
#    rs = new("jdbcResult", jresult=ret)
#})

#' @exportMethod dbExistsTable
setMethod("dbExistsTable", c("jdbcConnection", "character"), function(conn, name, schema=NULL) {
    name %in% dbListTables(conn, schema=schema)
})

#' @exportMethod dbReadTable
setMethod("dbReadTable", c("jdbcConnection", "character"), function(conn, name, schema=NULL, ...) {
    qname = if(!is.null(schema)) {
        paste0(dbQuoteIdentifier(conn, schema), ".", dbQuoteIdentifier(conn, name))
    } else {
        dbQuoteIdentifier(conn, name)
    }

    dbGetQuery(conn, paste0("SELECT * FROM ", qname))
})

setMethod("dbRemoveTable", c("jdbcConnection", "character"),
    function(conn, name, schema=NULL) {
    sql = if(is.null(schema))
        paste0("DROP TABLE ", dbQuoteIdentifier(conn, name))
    else 
        paste0("DROP TABLE ", dbQuoteIdentifier(conn, schema), ".",
               dbQuoteIdentifier(conn, name))
    ret = dbExecute(conn, sql)
    ret > 0
})

setMethod("dbWriteTable", c("jdbcConnection", "character", "data.frame"),
    function(conn, name, value, ...,
             row.names = FALSE, overwrite = FALSE, append = FALSE,
             field.types = NULL, temporary = FALSE,
             schema=NULL) {

    if (overwrite && append)
        stop("overwrite and append cannot both be TRUE", call. = FALSE)

    found = dbExistsTable(conn, name, schema=schema)
    if(found && overwrite)
        dbRemoveTable(conn, name, schema=schema)

    fcts = sapply(value, class) == "factor"
    value[fcts] = lapply(value[fcts], as.character)
    stopifnot(all(sapply(value, class) %in% c("character", "numeric", "integer")))

    if(!found || overwrite) {
        sql = sqlCreateTable(conn, name, value, field.types=field.types,
                             schema=schema, row.names=FALSE,
                             temporary=temporary)
        dbExecute(conn, sql)
    }

    if(nrow(value) > 0) {
        name = dbQuoteIdentifier(conn, name)
        fields = dbQuoteIdentifier(conn, names(value))

        params = rep("?", length(fields))
        sql = paste0(
            "INSERT INTO ", name, " (", paste0(fields, collapse = ", "), ")\n",
            "VALUES (", paste0(params, collapse = ", "), ")")

        stmt = dbSendStatement(conn, sql)
        dbBind(stmt, value)
    }

    invisible(TRUE)
})

