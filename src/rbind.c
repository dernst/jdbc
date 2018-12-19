#include <R.h>
#include <Rinternals.h>
#include <string.h>

SEXP jdbc_rbind(SEXP from, SEXP to) {
    
    for(int cur_col = 0; cur_col < LENGTH(to); cur_col++) {
        SEXP to_col = VECTOR_ELT(to, cur_col);
        int to_offset = 0;

        //Rprintf("col: %d of %d\n", cur_col+1, LENGTH(to));

        for(int cur_chunk = 0; cur_chunk < LENGTH(from); cur_chunk++) {
            SEXP from_col = VECTOR_ELT(VECTOR_ELT(from, cur_chunk), cur_col);

            /* TODO: replace sizeof(double) et al. */
            if(isReal(from_col)) {
                memcpy(REAL(to_col)+to_offset, REAL(from_col),
                       sizeof(double)*LENGTH(from_col));
            } else if(isInteger(from_col)) {
                memcpy(INTEGER(to_col)+to_offset, INTEGER(from_col),
                       sizeof(int)*LENGTH(from_col));
            } else if(isString(from_col)) {
               for(int i=0; i<LENGTH(from_col); i++) {
                   SET_STRING_ELT(to_col, to_offset+i, STRING_ELT(from_col, i));
               }
            } else {
                Rprintf("unknown column type?\n");
            }
            to_offset += LENGTH(from_col);
        }
    }

    return R_NilValue;
}


