package de.misc.jdbc;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ArrayBlockingQueue;





class StringResult {
    public StringResult(int r, int c, String v) {
        this.row = r;
        this.col = c;
        this.value = v;
    }
    public int row;
    public int col;
    public String value;
}

class StringResultProducer implements Runnable {
    //private StringResult
    //private ArrayBlockingQueue<StringResult> queue;
    private BulkRead reader;
    private int chunksize;
    public int rows_read = -1;

    public StringResultProducer(BulkRead br, int cs) {
        this.reader = br;
        this.chunksize = cs;
    }

    public void run() {
        //System.out.println("hello new thread");
        //TODO: store results/exceptions maybe
        try {
            int ret = this.reader.fetch(this.chunksize);
            this.rows_read = ret;
        } catch(Exception e) {
        }
        //System.out.println("new thread is done");
    }
}



public class BulkRead {
    private ResultSet rs;
    private int[] coltypes;

    //private BlockingQueue<ResultRow> queue;
    private BlockingQueue<StringResult> queue = null;

    public static final double NA_real = Double.longBitsToDouble(0x7ff00000000007a2L);
    public static final int NA_integer = -2147483648;


    public static void init(String sofn) {
        System.load(sofn);
    }


    public BulkRead(ResultSet rs, int[] coltypes) {
        this.rs = rs;
        this.coltypes = coltypes;

        //System.out.println("setting fetch size");
        try {
            rs.setFetchSize(4096);
            //System.out.println("fetch size is "+rs.getFetchSize());
        } catch (java.sql.SQLException e) {
            //System.out.println("cant set fetch size");
        }
    }


    public native void cb_set_int(int col, int row, int i);
    public native void cb_set_numeric(int col, int row, double i);
    public native void cb_set_string(int col, int row, String str);
    public native void cb_set_bytes(int col, int row, byte[] s);

    public int fetch_async(int chunksize) throws SQLException, IOException, InterruptedException {
        this.queue = new ArrayBlockingQueue<StringResult>(32);
        StringResultProducer srp = new StringResultProducer(this, chunksize);

        Thread t = new Thread(srp);
        t.start();

        while(true) {
            StringResult sr = this.queue.take();
            //System.out.println("parent got some");
            if(sr.col < 0)
                break;
            cb_set_string(sr.col, sr.row, sr.value);
        }

        //System.out.println("ok done");

        t.join();

        //System.out.println("rows_read: " + srp.rows_read);
        return srp.rows_read;
    }

    public int fetch(int chunksize) throws SQLException, IOException, InterruptedException {
        int row = 0;

        while(row < chunksize && this.rs.next()) {
            for(int col=0; col<coltypes.length; col++) {
                //integer
                if(coltypes[col] == 1) {
                    int val = this.rs.getInt(col+1);
                    if(rs.wasNull())
                        val = NA_integer;
                    cb_set_int(col, row, val);
                // numeric
                } else if(coltypes[col] == 2) {
                    double val = this.rs.getDouble(col+1);
                    if(rs.wasNull())
                        val = NA_real;
                    cb_set_numeric(col, row, val);
                } else {
                    /*
                    String val = this.rs.getString(col+1);
                    byte[] v = val != null ? val.getBytes("UTF-8") : null;
                    cb_set_bytes(col, row, v);
                    */
                    String val = this.rs.getString(col+1);
                    cb_set_string(col, row, val);

                    /*
                    if(val != null) {
                        if(this.queue != null) {
                            StringResult sr = new StringResult(row, col, val);
                            this.queue.put(sr);
                        } else {
                            cb_set_string(col, row, val);
                        }
                    }
                    */
                }
            }

            row++;
        }

        if(this.queue != null) {
            this.queue.put(new StringResult(-1,-1,null));
        }

        return row;
    }

    public int fetchAllVoid() throws SQLException {
        int count=0;
        while(this.rs.next())
            count++;
        return count;
    }

    public int fetch3(int chunksize) throws SQLException, IOException {
        int row = 0;
        int str_cols=0;

        for(int col=0; col<coltypes.length; col++) {
            if(coltypes[col] != 1)
                str_cols=0;
        }
        
        StringBuffer[] sb = new StringBuffer[str_cols];
        for(int col=0; col<str_cols; col++) {
            sb[col] = new StringBuffer();
        }

        while(row < chunksize && this.rs.next()) {
            int str_col_offset = 0;
            for(int col=0; col<coltypes.length; col++) {
                if(coltypes[col] == 1) {
                    int val = this.rs.getInt(col+1);
                    //cb_set_int(col, row, val);
                } else {
                    sb[str_col_offset].append(this.rs.getString(col+1));
                    str_col_offset++;
                    /*
                    String val = this.rs.getString(col+1);
                    byte[] v = val != null ? val.getBytes("UTF-8") : null;
                    cb_set_bytes(col, row, v);
                    */
                    /*
                    String val = this.rs.getString(col+1);
                    cb_set_string(col, row, val);
                    */
                }
            }

            row++;
        }

        return row;
    }

}


