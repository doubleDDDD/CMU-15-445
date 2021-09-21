# bin/sqlite3是shell.c编译的结果
# 所以如果用c++来编程访问sqlite3的话，嵌入式的，与levelDB是一样的
# 全部直接link进来就完事了，然后代码中一顿open以及其它操作等

CXX = g++ -std=c++11

DIR_CUR = $(shell pwd)
DIR_SRC = $(DIR_CUR)/../src
DIR_SQLITE3_INCLUDE = $(DIR_SRC)/include/sqlite
DIR_SQLITE3_LIB = $(DIR_CUR)/../debug/lib

# 这里目前并没有加载虚拟表
CFLAGS := -g -Wall -O0 -Wno-unused-variable
INCLUDEFLAGS += -I$(DIR_SQLITE3_INCLUDE)
# -L表示的是库的路径
LDFLAGS := -L$(DIR_SQLITE3_LIB) -lsqlite3 -lvtable -Wl,-rpath=$(DIR_SQLITE3_LIB)
LDFLAGS += -lpthread -lunwind -lunwind-x86_64 -ldl

BIN = sql3
SRCS = $(wildcard *.cpp)
DEPS = $(SRCS:.cpp=.d)
OBJS = $(patsubst %.cpp, %.o, $(SRCS))

RM = rm -rf

.PHONY:
all: $(BIN)

$(BIN): $(OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

%.o:%.cpp
	$(CXX) -c $< -o $@ $(CFLAGS) $(INCLUDEFLAGS)

%.d:%.cpp
	@set -e; \
	$(RM) $@; \
	$(CXX) -MM $< $(INCLUDEFLAGS) > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@: ,g' < $@.$$$$ > $@; \
	$(RM) $@.$$$$
-include $(DEPS)

.PHONY:test
test:
	@echo do check

.PHONY:clean
clean:
	@$(RM) *.d *.o $(BIN)
	@echo clean done