#include <R.h>
#include <Rinternals.h>

#include <jni.h>


SEXP current_df = NULL;
char *string_buffer = NULL;
int string_buffer_size = 0;

void cb_set_numeric(JNIEnv *env, jobject obj, jint col, jint row, jdouble num); 
void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num); 
void cb_set_string(JNIEnv *env, jobject obj, jint col, jint row, jstring str);
void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str);

jdouble cb_get_numeric(JNIEnv *env, jobject obj, jint col, jint row); 
jint cb_get_int(JNIEnv *env, jobject obj, jint col, jint row); 
jstring cb_get_string(JNIEnv *env, jobject obj, jint col, jint row);


static void string_buffer_ensure_size(int);

static int register_natives(JNIEnv *env) {
    // BulkRead
    jclass cls = (*env)->FindClass(env, "de/misc/jdbc/BulkRead");

    JNINativeMethod my_natives[4];
    my_natives[0].name = "cb_set_int";
    my_natives[0].signature = "(III)V";
    my_natives[0].fnPtr = (void*)cb_set_int;

    my_natives[1].name = "cb_set_string";
    my_natives[1].signature = "(IILjava/lang/String;)V";
    my_natives[1].fnPtr = (void*)cb_set_string;

    my_natives[2].name = "cb_set_bytes";
    my_natives[2].signature = "(II[B)V";
    my_natives[2].fnPtr = (void*)cb_set_bytes;

    my_natives[3].name = "cb_set_numeric";
    my_natives[3].signature = "(IID)V";
    my_natives[3].fnPtr = (void*)cb_set_numeric;

    if((*env)->RegisterNatives(env, cls, my_natives, 4) != 0) {
        Rprintf("RegisterNatives failed\n");
        return 0;
    }

    cls = (*env)->FindClass(env, "de/misc/jdbc/BulkWrite");

    my_natives[0].name = "cb_get_int";
    my_natives[0].signature = "(II)I";
    my_natives[0].fnPtr = (void*)cb_get_int;

    my_natives[1].name = "cb_get_string";
    my_natives[1].signature = "(II)Ljava/lang/String;";
    my_natives[1].fnPtr = (void*)cb_get_string;

    my_natives[2].name = "cb_get_numeric";
    my_natives[2].signature = "(II)D";
    my_natives[2].fnPtr = (void*)cb_get_numeric;

    if((*env)->RegisterNatives(env, cls, my_natives, 3) != 0) {
        Rprintf("RegisterNatives (BulkWrite) failed\n");
        return 0;
    }


    return 1;
}



JNIEXPORT jint JNICALL
JNI_OnLoad(JavaVM *jvm, void *reserved) {
    jint res;
    JNIEnv *env=NULL;
    //puts("JNI_OnLoad was here");

    res = (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(res != 0) {
        Rprintf("AttachCurrentThread failed\n");
        goto bail;
    }

    if(!register_natives(env)) {
        return 0;
    }

bail:    
    return JNI_VERSION_1_8;
}

JNIEnv* getEnv() {
    JavaVM *jvm = NULL;
    JNIEnv *env = NULL;
    jsize l;
    jint res=-1;

    res = JNI_GetCreatedJavaVMs(&jvm, 1, &l);
    if(res != 0) {
        Rprintf("JNI_GetCreatedJavaVMs failed\n");
        return NULL;
    }
    if(l < 1) {
        Rprintf("no java VM yet\n");
        return NULL;
    }

    res = (*jvm)->AttachCurrentThread(jvm, (void*)&env, 0);
    if(res != 0) {
        Rprintf("AttachCurrentThread failed\n");
        return NULL;
    }


    return env;
}


SEXP jdbc_set_df(SEXP df) {
    current_df = df;
    return R_NilValue;
}

SEXP jdbc_cleanup() {
    current_df = R_NilValue;
    if(string_buffer != NULL) {
        free(string_buffer);
    }
    string_buffer = NULL;
    string_buffer_size = -1;
    return R_NilValue;
}

jdouble cb_get_numeric(JNIEnv *env, jobject obj, jint col, jint row) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    return (jdouble) REAL(r_col)[row];
}

jint cb_get_int(JNIEnv *env, jobject obj, jint col, jint row) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    return (jint) INTEGER(r_col)[row];
}

jstring cb_get_string(JNIEnv *env, jobject obj, jint col, jint row) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    SEXP s = STRING_ELT(r_col, row);
    const char *c = CHAR(s);
    //TODO: which is the correct test? lol
    if(s == NULL || s == NA_STRING || c == NULL) {
        //Rprintf("NULL in get_string\n");
        return NULL;
    }

    jstring ret = (*env)->NewStringUTF(env, c);
    return ret;
}


void cb_set_int(JNIEnv *env, jobject obj, jint col, jint row, jint num) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    INTEGER(r_col)[row] = num;
    return;
}

void cb_set_numeric(JNIEnv *env, jobject obj, jint col, jint row, jdouble num) {
    SEXP r_col = VECTOR_ELT(current_df, col);
    REAL(r_col)[row] = num;
    return;
}


void cb_set_string(JNIEnv *env, jobject obj, jint col, jint row, jstring str) {
    if(str == NULL) {
        //puts("> str NULL");
        SEXP r_col = VECTOR_ELT(current_df, col);
        SET_STRING_ELT(r_col, row, NA_STRING);
        return;
    }

    // jchar is 16-byte per character :/
    if(0) {
        jboolean iscopy;

        const jchar *c = (*env)->GetStringCritical(env, str, &iscopy);
        int len = strlen((const char*)c);

        if(iscopy) {
            Rprintf("is copy:(\n");
        }

        SEXP r_col = VECTOR_ELT(current_df, col);
        SEXP foo = mkCharLenCE((const char*)c, len, CE_UTF8);
        SET_STRING_ELT(r_col, row, foo);

        (*env)->ReleaseStringCritical(env, str, c);
    }

    // TODO: perhaps we don't have to retrieve both lengths?!
    int len = (*env)->GetStringUTFLength(env, str);
    string_buffer_ensure_size(len+1);

    int blen = (*env)->GetStringLength(env, str);
    (*env)->GetStringUTFRegion(env, str, 0, blen, (char*)string_buffer);

    SEXP r_col = VECTOR_ELT(current_df, col);
    SEXP foo = mkCharLenCE(string_buffer, len, CE_UTF8);
    SET_STRING_ELT(r_col, row, foo);

    return;
}

static void string_buffer_ensure_size(int s) {
    const int block_size = 4096;
    //int new_size = s - (s % block_size) * block_size;

    if(string_buffer == NULL) {
        string_buffer_size = (s / block_size + 1) * block_size;
        if(0) Rprintf("allocating s=%d, new size=%d\n", s, string_buffer_size);
        string_buffer = (char*) malloc(sizeof(char) * string_buffer_size);
    } else if(string_buffer_size < s){
        if(0) Rprintf("growing buffer for s=%d (current size=%d)\n", s, string_buffer_size);
        string_buffer_size = (s / block_size + 1) * block_size;
        string_buffer = realloc(string_buffer, sizeof(char)*string_buffer_size);
    }
}

void cb_set_bytes(JNIEnv *env, jobject obj, jint col, jint row, jbyteArray str) {
    if(str == NULL) {
        return;
    }

    int len = (*env)->GetArrayLength(env, str);
    string_buffer_ensure_size(len+1);

    (*env)->GetByteArrayRegion(env, str, 0, len, (jbyte*)string_buffer);

    SEXP r_col = VECTOR_ELT(current_df, col);
    SEXP foo = mkCharLenCE(string_buffer, len, CE_UTF8);
    SET_STRING_ELT(r_col, row, foo);
}





SEXP test_bytebuffer(SEXP *df) {
    //JNIEnv *env = getEnv();


    //jobject bb = (*env)->NewDirectByteBuffer(env, (void*) data, sizeof(MyNativeStruct));

    return R_NilValue;
}






