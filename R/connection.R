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
    new("jdbcResult", jresult=rs)
})

#' @exportMethod dbQuoteIdentifier
setMethod("dbQuoteIdentifier", c("jdbcConnection", "character"), function(conn, x, ...) {
    md = .jcall(conn@jconn, "Ljava/sql/DatabaseMetaData;", "getMetaData")
    qs = .jcall(md, "S", "getIdentifierQuoteString")
    SQL(paste0(qs, x, qs), names=x)
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

