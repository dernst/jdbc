#' jdbc results class.
#' 
#' @keywords internal
#' @export
setClass("jdbcResult", 
    contains = "DBIResult",
    slots = list(jresult = 'jobjRef', env='environment'),
    prototype = list(jresult = .jnull(), env=new.env(TRUE, emptyenv()))
)

#' Bind data to a statement
#' @export
setMethod("dbBind", "jdbcResult", function(res, params=list(), batchsize=4096L, ...) {
    stopifnot(is.list(params))
    lengths = sapply(params, length)
    stopifnot(all(lengths == lengths[1]))
    stopifnot(all(sapply(params, class) %in% c("integer", "numeric", "character", "factor")))


    ct = sapply(params, class)
    params[ct == "factor"] = lapply(params[ct == "factor"], as.character)
    coltypes = with(list(ct=sapply(params, class)),
                    ifelse(ct == "integer", 1L,
                    ifelse(ct == "numeric", 2L, 9L)))


    .Call("jdbc_set_df", params)

    bw = .jnew("de/misc/jdbc/BulkWrite",res@jresult,
               .jarray(coltypes), lengths[1])
    rows_affected = .jcall(bw, "I", "execute", as.integer(batchsize))
    assign("rows_affected", rows_affected, envir=res@env)

    return(rows_affected)

    ps = res@jresult
    for(i in seq_along(params)) {
        idx = i-1L
        p = params[[i]]
        if(class(p) == "integer") {
            if(is.na(p)) {
                .jcall(ps, "V", "setNull", idx, 4L)
            } else {
                .jcall(ps, "V", "setInt", idx, p)
            }
        } else if(class(p) == "numeric") {
            if(is.na(p)) {
                .jcall(ps, "V", "setNull", idx, 6L) #TODO: check this
            } else {
                .jcall(ps, "V", "setDouble", idx, p)
            }
        } else {
            if(is.na(p))
                .jcall(ps, "V", "setNull", idx, 1L)
            else
                .jcall(ps, "V", "setString", idx, p)
        }
    }
   
    rs = .jcall(ps, "I", "executeUpdate")
    rs
})

#' @export
setMethod("dbGetRowsAffected", "jdbcResult", function(res, ...) {
    #TODO: test if this was already executed? would it make sense
    # to cache the result and only call executeUpdate if the
    # result is not cached yet?
    if(exists("rows_affected", envir=res@env))
        return(get("rows_affected", envir=res@env))
    rs = .jcall(res@jresult, "I", "executeUpdate")
    rs
})

#' @export
setMethod("dbClearResult", "jdbcResult", function(res, ...) {
    # free resources
    .jcall(res@jresult, "V", "close")
    invisible(TRUE)
})

#' Retrieve records from jdbc query
#' @export
setMethod("dbFetch", "jdbcResult", function(res, n = -1L, ...) {
    stopifnot(length(n) == 1)
    if(n < -1L)
        stop("invalid value of n")
    if(is.infinite(n)) n = -1L
    stopifnot(as.integer(n) == n)

    ret = jdbc_get_query(res, n=n)
    ret
})


#' @export
setMethod("dbHasCompleted", "jdbcResult", function(res, ...) {
    .jcall(res@jresult, "Z", "isClosed")
})


