package de.misc.jdbc;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class BulkWrite {
    private PreparedStatement stmt;
    private int[] coltypes;
    private int nrows;

    private native int cb_get_int(int ncol, int nrow);
    private native double cb_get_numeric(int ncol, int nrow);
    private native String cb_get_string(int ncol, int nrow);


    public BulkWrite(PreparedStatement s, int[] coltypes, int nrows) {
        this.stmt = s;
        this.coltypes = coltypes;
        this.nrows = nrows;
    }

    public void execute(int batchsize) throws SQLException {
        int batch=0;
        for(int row=0; row<nrows; row++) {
            for(int col=0; col<coltypes.length; col++) {
                if(coltypes[col] == 1) {
                    stmt.setInt(col+1, cb_get_int(col, row));
                } else if(coltypes[col] == 2) {
                    stmt.setDouble(col+1, cb_get_numeric(col, row));
                } else {
                    String v = cb_get_string(col, row);
                    if(v != null)
                        stmt.setString(col+1, v);
                    else {
                        //System.out.println("set NULL string");
                        stmt.setNull(col+1, Types.VARCHAR);
                    }
                }
            }

            stmt.addBatch();

            if(++batch >= batchsize) {
                stmt.executeBatch();
                batch=0;
            }
        }
        stmt.executeBatch();
    }
};

