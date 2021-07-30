##################################################################################
# SRC CMAKELISTS
##################################################################################

# https://raw.githubusercontent.com/azadkuh/sqlite-amalgamation/master/CMakeLists.txt
string(REGEX MATCH "Clang"  IS_CLANG "${CMAKE_CXX_COMPILER_ID}")
string(REGEX MATCH "GNU"    IS_GNUXX "${CMAKE_CXX_COMPILER_ID}")
string(REGEX MATCH "Linux"  IS_LINUX "${CMAKE_SYSTEM_NAME}")
string(REGEX MATCH "Darwin" IS_MACOS "${CMAKE_SYSTEM_NAME}")


# sqlite3 compile options, modules, ...
option(BUILD_ENABLE_DBSTAT_VTAB "enables dbstat virtual table"                 OFF)
option(BUILD_ENABLE_FTS3        "enables full text searches version 3"         OFF)
option(BUILD_ENABLE_FTS5        "enables full text searches version 5"         OFF)
option(BUILD_ENABLE_ICU         "enables international components fir unicode" OFF)
option(BUILD_ENABLE_JSON1       "enables JSON SQL functins"                    OFF)
option(BUILD_ENABLE_RBU         "enables resumable bulk update extension"      OFF)
option(BUILD_ENABLE_RTREE       "enables R*TRee index extension"               OFF)

# other build options
option(BUILD_SHARED_LIBS "build sqlite3 as a unix shared (so/dylib) library" ON)
option(BUILD_SHELL       "build sqlite3 shell application"                   ON)
if(MSVC)
    option(BUILD_MT_RELEASE "static msvcrt build" ON)
endif()

# compiler settings
if(IS_CLANG OR IS_GNUXX)
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -O3 -g0 -Wall -Wextra -pedantic -Wcast-align -Wunused -Wno-unused-parameter")
elseif(MSVC)
    set(CMAKE_C_FLAGS_RELEASE "-nologo -Zc:wchar_t -FS -O2 -MD -Zc:strictStrings -W3")
    set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG} -MDd")
    if(BUILD_MT_RELEASE)
        string(REPLACE "-MD"  "-MT"  CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
        string(REPLACE "-MDd" "-MTd" CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}")
    endif()
    set(CMAKE_SHARED_LINKER_FLAGS_RELEASE
            "${CMAKE_SHARED_LINKER_FLAGS_RELASE} \
            /Gy /GF /OPT:REF /OPT:ICF")
    if(BUILD_SHARED_LIBS)
        message(SEND_ERROR "@error: shared lib is not supported under Visual Studio.\n \
                please compile as a static lib.")
    endif()
endif()

if(BUILD_ENABLE_DBSTAT_VTAB)
    add_definitions(-DSQLITE_ENABLE_DBSTAT_VTAB)
endif()
if(BUILD_ENABLE_FTS3)
    add_definitions(-DSQLITE_ENABLE_FTS3)
endif()
if(BUILD_ENABLE_FTS5)
    add_definitions(-DSQLITE_ENABLE_FTS5)
endif()
if(BUILD_ENABLE_ICU)
    add_definitions(-DSQLITE_ENABLE_ICU)
endif()
if(BUILD_ENABLE_JSON1)
    add_definitions(-DSQLITE_ENABLE_JSON1)
endif()
if(BUILD_ENABLE_RBU)
    add_definitions(-DSQLITE_ENABLE_RBU)
endif()
if(BUILD_ENABLE_RTREE)
    add_definitions(-DSQLITE_ENABLE_RTREE)
endif()
# Multi-thread.
# In this mode, SQLite can be safely used by multiple threads provided that no single
# database connection is used simultaneously in two or more threads.
add_definitions(-DSQLITE_THREADSAFE=2)

# sqlite3 as library
# 依赖于src/sqlit下的c代码 生成动态库 libsqlite3.so
# BUILD_SHARED_LIBS 以及 BUILD_SHELL 均为 ON
# 对于 target_link_libraries，第一个参数是
add_library(sqlite3 sqlite/sqlite3.c include/sqlite/sqlite3ext.h include/sqlite/sqlite3.h)
if(IS_LINUX)
    if(BUILD_SHARED_LIBS)
        # sqlite3 这个 .so 本身会  link pthread 以及 dl 库，这两个库都是默认搜索路径下的库，也无需自行指定lib的搜索路径
        target_link_libraries(sqlite3 pthread dl unwind unwind-x86_64)  # 只写库名字就ok，cmake自己知道库的定义规则，并且知道如何 find
    endif()
elseif(IS_MACOS AND BUILD_SHARED_LIBS)
    set(CMAKE_SKIP_RPATH 0) # make dynamic linking work for Mac
endif()

# shell app
if(BUILD_SHELL)
    # message(STATUS "it is ${BUILD_SHELL}")
    add_executable(sqlite3_shell sqlite/shell.c)
    target_link_libraries(sqlite3_shell sqlite3)  # 在sqlite3_shell中链接模块 libsqlite3.so
    set_target_properties(sqlite3_shell PROPERTIES OUTPUT_NAME sqlite3)

    if(IS_LINUX)
        if(NOT BUILD_SHARED_LIBS)
            target_link_libraries(sqlite3_shell pthread dl unwind unwind-x86_64)
        endif()
    endif()
endif()

# --- [ sqlite_vtable
file(GLOB_RECURSE srcs ${PROJECT_SOURCE_DIR}/src/*/*.cpp)
file(GLOB sqlite_srcs ${PROJECT_SOURCE_DIR}/src/sqlite/*.c)
list(REMOVE_ITEM srcs ${sqlite_srcs})
# message(STATUS "it is ${srcs}")
# 依赖于src下除sqlit外的所有cpp 代码生成动态库 libvtable.so 这个实际上就相当于是数据库的内核
# sqlit起来之后，加载libvtable.so，之后的数据库操作就会由libvtable.so来接管，这个就可以学习数据库 后端引擎 的核心思想了
add_library(vtable SHARED ${srcs})
if(IS_LINUX)
    if(BUILD_SHARED_LIBS)
        target_link_libraries(vtable unwind unwind-x86_64)  # 只写库名字就ok，cmake自己知道库的定义规则，并且知道如何 find
    endif()
elseif(IS_MACOS AND BUILD_SHARED_LIBS)
    set(CMAKE_SKIP_RPATH 0)
endif()