#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <unistd.h>
#include <libunwind.h>
#include <cxxabi.h>
#include <iostream>
#include <cassert>
#include <cstdlib>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <sqlite3.h>
// 下面的是一个多余的include，会造成这样的一个报错 error: ‘sqlite3_api’ was not declared in this scope
// #include <sqlite3ext.h>

#define gettid() syscall(__NR_gettid)

// for c
void
BackTrace() 
{
    unw_cursor_t cursor;
    unw_context_t context;

    // Initialize cursor to current frame for local unwinding.
    unw_getcontext(&context);
    unw_init_local(&cursor, &context);

    // Unwind frames one by one, going up the frame stack.
    while (unw_step(&cursor) > 0) {
        unw_word_t offset, pc;
        unw_get_reg(&cursor, UNW_REG_IP, &pc);
        if (pc == 0) {break;}
        std::printf("0x%lx:", pc);

        char sym[256];
        if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
            std::printf(" (%s+0x%lx)\n", sym, offset);
        } else {
            std::printf(" -- error: unable to obtain symbol name for this frame\n");
        }
    }
}

// for c++
void 
BackTracePlus() 
{
    unw_cursor_t cursor;
    unw_context_t context;

    // Initialize cursor to current frame for local unwinding.
    unw_getcontext(&context);
    unw_init_local(&cursor, &context);

    // Unwind frames one by one, going up the frame stack.
    while (unw_step(&cursor) > 0) {
        unw_word_t offset, pc;
        unw_get_reg(&cursor, UNW_REG_IP, &pc);
        if (pc == 0) {break;}
        std::printf("0x%lx:", pc);

        char sym[256];
        if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
            char* nameptr = sym;
            int status;
            char* demangled = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
            if (status == 0) {
                nameptr = demangled;
            }
            std::printf(" (%s+0x%lx)\n", nameptr, offset);
            std::free(demangled);
        } else {
            std::printf(" -- error: unable to obtain symbol name for this frame\n");
        }
    }
}

static int 
Callback(void *NotUsed, int argc, char **argv, char **azColName)
{
    int i;
    for(i=0; i<argc; i++){
        printf("%s=%s, ", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

void
RealTable()
{
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    const char* data = "Callback function called";
    
    rc = sqlite3_open("test.db", &db);
    if(rc){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(0);
    } else { fprintf(stderr, "Opened database successfully\n"); }

    // 先创建一张表
    /* Create SQL statement */
    const std::string sqlcreate = "CREATE TABLE COMPANY("  \
            "ID INT PRIMARY KEY     NOT NULL," \
            "NAME           TEXT    NOT NULL," \
            "AGE            INT     NOT NULL," \
            "ADDRESS        CHAR(50)," \
            "SALARY         REAL );";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlcreate.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Table created successfully\n"); }

    // 执行insert
    /* Create SQL statement */
    const std::string sqlinsert = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
            "VALUES (1, 'Paul', 32, 'California', 20000.00 ); " \
            "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
            "VALUES (2, 'Allen', 25, 'Texas', 15000.00 ); "     \
            "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
            "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );" \
            "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
            "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlinsert.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Records created successfully\n"); }

    // 读操作
    /* Create SQL statement */
    const std::string sqlselect = "SELECT * from COMPANY";
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlselect.c_str(), Callback, (void*)data, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }else { fprintf(stdout, "Operation done successfully\n"); }

    // update 操作
    /* Create merged SQL statement */
    const std::string sqlupdate = "UPDATE COMPANY set SALARY = 88888.00 where ID=1; " \
            "SELECT * from COMPANY where ID=1;";
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlupdate.c_str(), Callback, (void*)data, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Operation done successfully\n"); }

    sqlite3_close(db);
    return;
}

void
UpdateAndRead(sqlite3 *db)
{
    int rc;
    char *zErrMsg = 0;
    // sleep(2);
    std::printf("update and read threadid=%ld, T1\n", gettid());

    /**
     * @brief sqlite3不会出问题
     * update之后的读取都有问题
     * 只能修bug
     */
    // const std::string sqlupdateandread = \
    //         "UPDATE COMPANY set SALARY=88888 where ID=1; " \
    //         "SELECT * from COMPANY where ID=1;";

    const std::string sqlupdateandread = "UPDATE COMPANY set SALARY=88888 where ID=1;";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlupdateandread.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    return;
}

void
Update(sqlite3 *db)
{
    return;
    int rc;
    char *zErrMsg = 0;
    // sleep(4);
    std::printf("update threadid=%ld, T2\n", gettid());

    const std::string sqlupdateandread = "UPDATE COMPANY set SALARY=66666 where ID=1;";
    
    // rc = sqlite3_exec(db, sqlupdateandread.c_str(), Callback, 0, &zErrMsg);
    // if( rc != SQLITE_OK ){
    //     fprintf(stderr, "SQL error: %s\n", zErrMsg);
    //     sqlite3_free(zErrMsg);
    // }
    return;
}

void
VTable()
{
    // db是一个数据库连接的实例
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    const char* data = "Callback function called";

    // If the filename is ":memory:", then a private, temporary in-memory database is created for the connection
    // if( data.zDbFilename==0 ){ data.zDbFilename = ":memory:"; }
    rc = sqlite3_open(":memory:", &db);
    if(rc){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(0);
    } else { fprintf(stderr, "Open database successfully\n"); }
    // 到此为止数据库连接的实例显然是已经创建完成了

    // load 虚拟表
    // const std::string extpath = "/root/CMU-15-445/Lab/debug/lib/libvtable.so";
    const std::string extpath = "/home/doubled/double_D/DB/CMU-15-445/Lab/debug/lib/libvtable.so";
    rc = sqlite3_enable_load_extension(db, 1);
    // int sqlite3_load_extension(
    // sqlite3 *db,          /* Load the extension into this database connection */
    // const char *zFile,    /* Name of the shared library containing extension */
    // const char *zProc,    /* Entry point.  Derived from zFile if 0 */
    // char **pzErrMsg       /* Put error message here if not 0 */
    // );
    // load扩展之前必须有一个数据库的连接
    rc = sqlite3_load_extension(db, extpath.c_str(), NULL, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Load extension successfully\n"); }

    // 先创建一张虚拟表，会生成以下文件 vtable.db  vtable.log
    // 创建表之后再关闭数据库，是会调用到xDisconnect的
    // 会调用 VtabCreate
    /* Create SQL statement */
    const std::string sqlcreate = "CREATE VIRTUAL TABLE COMPANY " \
        "USING vtable("\
        "'ID int, "\
        "NAME varchar(24), "\
        "AGE int, "\
        "ADDRESS varchar(48), "\
        "SALARY int');";

    /* Execute SQL statement */
    // std::printf("sql is %s\n", sqlcreate.c_str());    
    rc = sqlite3_exec(db, sqlcreate.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Virtual table created successfully\n"); }

    // 执行insert
    /* Create SQL statement */
    const std::string sqlinsert = \
            "INSERT INTO COMPANY VALUES(1, 'Paul', 32, 'California', 20000); " \
            "INSERT INTO COMPANY VALUES(2, 'Allen', 25, 'Texas', 15000 ); "     \
            "INSERT INTO COMPANY VALUES(3, 'Teddy', 23, 'Norway', 20000 ); " \
            "INSERT INTO COMPANY VALUES(4, 'Mark', 25, 'Rich-Mond ', 65000 );";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlinsert.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else { fprintf(stdout, "Records created successfully\n"); }

    // // 再来一个读操作
    // /* Create SQL statement */
    // const std::string sqlselect = "SELECT * from COMPANY;";
    // /* Execute SQL statement */
    // rc = sqlite3_exec(db, sqlselect.c_str(), Callback, (void*)data, &zErrMsg);
    // if( rc != SQLITE_OK ){
    //     fprintf(stderr, "SQL error: %s\n", zErrMsg);
    //     sqlite3_free(zErrMsg);
    // }else{
    //     fprintf(stdout, "Select Operation done successfully\n");
    // }

    /**
     * @brief 准备测试一下并发
     *  T1 写 + 读
     *  T2 写
     *  不加以控制 T1将得到错误的数据
     */
    // std::thread threads[2];
    // threads[0] = std::thread(UpdateAndRead, db);
    // threads[1] = std::thread(Update, db);
    // for (auto& t: threads) {t.join();}

    const std::string sqlupdateandread = "UPDATE COMPANY set SALARY=88888 where ID=1;";
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlupdateandread.c_str(), Callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    // 最后再验证一下
    std::printf("last verify\n");
    const std::string sqlselectone = "SELECT * from COMPANY;";
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sqlselectone.c_str(), Callback, (void*)data, &zErrMsg);
    if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    sqlite3_close(db);
    return;
}

// 这个参考的就是菜鸟教程
int main(int argc, char* argv[])
{
    // RealTable();
    VTable();
    return 0;
}