#' @import rJava
#' @useDynLib jdbc
NULL

#' @export dbGetStatement dbWithTransaction dbQuoteString dbCanConnect
#' @export dbExecute dbAppendTable dbBegin dbIsReadOnly
#' @export dbListObjects dbCreateTable dbConnect dbColumnInfo dbReadTable
#' @export dbCommit dbRemoveTable dbDisconnect
NULL

.onLoad = function(libname, pkgname) {
    .jinit()
    .jaddClassPath(system.file("java", "jdbc.jar", package="jdbc"))

    shlib = paste0("jdbc", .Platform$dynlib.ext)
    #TODO: is there a better way to find out that shlibs are in x64/ subfolder?
    if(.Platform$OS.type == "windows")
        shlib = paste0("x64/", shlib)
    fn = system.file("libs", shlib, package="jdbc")
    if(!file.exists(fn)) {
        fn = system.file("src", shlib, package="jdbc")
        if(!file.exists(fn))
            stop(paste0("cant find ", shlib))
    }

    .jcall("de/misc/jdbc/BulkRead", "V", "init", fn)
}

get_column_types = function(md) {
    numcols <- .jcall(md, "I", "getColumnCount")
    cts = sapply(seq_len(numcols), function(x) .jcall(md, "I", "getColumnType", x))
    # see here (e.g.):
    # http://developer.classpath.org/doc/java/sql/Types-source.html
    #ifelse(cts == 4, "integer", "character")
    ifelse(cts %in% c(4, 5, -6), "integer",
    ifelse(cts %in% c(6, 7, 8), "numeric",
           "character"))
}

get_column_names = function(md) {
    numcols = .jcall(md, "I", "getColumnCount")
    lbls = sapply(seq_len(numcols), function(x) .jcall(md, "S", "getColumnLabel", x))
    lbls
}

jdbc_get_query = function(res, n=-1L, chunksize=4096L) {
    stopifnot(is(res, "jdbcResult"))
    rs = res@jresult
    md <- .jcall(res@jresult, "Ljava/sql/ResultSetMetaData;", "getMetaData")

    #if(.jcall(rs, "Z", "isClosed")) {
    #    stop("result set is already closed")
    #}

    # workaround for DBItest? test if it only works for sqlite
    .jcall(md, "I", "getColumnCount", check=FALSE)
    if(.jgetEx(TRUE) != .jnull()) {
        warning("update statements dont return a result")
        return(data.frame())
    }
       

    # TODO: make configurable: chunksize and chunk growth factor
    # TODO: take `n` into account
    list_of_dfs = list()
    length(list_of_dfs) = 16L
    df_offset = 1L
    coltypes = get_column_types(md)
    col_names = get_column_names(md)
    rows_read = 0L
    chunksize = as.integer(chunksize)
    n = as.integer(n)

    return_empty = function() {
        tmp = lapply(coltypes, function(x) vector(mode=x, length=0))
        names(tmp) = col_names
        attr(tmp, "row.names") <- .set_row_names(0L)
        class(tmp) <- "data.frame"
        return(tmp)
    }

    if(n == 0L) {
        return(return_empty())
    }

    argct = .jarray(ifelse(coltypes=="integer", 1L,
                    ifelse(coltypes=="numeric", 2L, 9L)))
    br = .jnew("de/misc/jdbc/BulkRead", rs, argct)

    while(!.jcall(rs, "Z", "isClosed")) {
        chunk = lapply(coltypes, function(x) {
            vector(mode=x, length=chunksize)
        })

        .Call("jdbc_set_df", chunk)

        #t0 = Sys.time()
        if(n > 0) {
            chunksize_here = min(max(0L, n - rows_read), chunksize)
            if(chunksize_here < 1L) {
                break
            }
            ret = .jcall(br, "I", "fetch", chunksize_here)
        } else {
            #ret = .jcall(br, "I", "fetch_async", chunksize)
            ret = .jcall(br, "I", "fetch", chunksize)
        }
        #t1 =  Sys.time()

        rows_read = rows_read + ret
        if(FALSE) {
        cat(sprintf("%.2fM rows read in %.2fs\n",
                    rows_read/1e6,
                    as.numeric(t1-t0, units="secs")))
        }

        # truncate chunk if necessary
        if(ret < chunksize) {
            for(i in seq_along(chunk)) {
                length(chunk[[i]]) = ret
            }
        }

        #attr(chunk, "row.names") <- c(NA_integer_, length(chunk[[1]]))
        attr(chunk, "row.names") <- .set_row_names(length(chunk[[1]]))
        class(chunk) <- "data.frame"

        if(df_offset > length(list_of_dfs))
            length(list_of_dfs) = length(list_of_dfs)*2L

        list_of_dfs[[df_offset]] = chunk
        df_offset = 1L+df_offset

        chunksize = chunksize*2L
    }

    .Call("jdbc_cleanup")

    # jdbc_rbind is slightly slower, might need some looking at later
    list_of_dfs = list_of_dfs[!sapply(list_of_dfs, is.null)]
    if(length(list_of_dfs) < 1L)
        return(return_empty())
    ret = jdbc_rbind(list_of_dfs)
    names(ret) = col_names
    rownames(ret) = seq.int(nrow(ret))
    ret
}

jdbc_rbind = function(lst, coltypes=sapply(lst[[1]], class)) {
    newlen = sum(sapply(lst, nrow))
    to = lapply(coltypes, function(x) {
        vector(mode=x, length=newlen)
    })

    #attr(to, "row.names") <- c(NA_integer_, newlen)
    attr(to, "row.names") <- .set_row_names(newlen)
    class(to) <- "data.frame"

    .Call("jdbc_rbind", lst, to)
    to
}


