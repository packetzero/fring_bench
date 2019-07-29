CMAKE_MINIMUM_REQUIRED(VERSION 2.7)

set (PROJECT_NAME encot)
PROJECT(${PROJECT_NAME})

set(CMAKE_SUPPRESS_REGENERATION true)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF) # want c++11 not gnu++11

set(CMAKE_CXX_RELEASE_FLAGS "-DNDEBUG=1")

set(DEPDIR $ENV{DEPDIR})

# find dependencies
FIND_LIBRARY(GTEST_LIB gtest HINT ${DEPDIR}/lib )

file(GLOB HDRS "*.h*")
file(GLOB SRCS "*.cpp")

include_directories(../include )
include_directories(.. $ENV{DEPDIR}/include )
include_directories(.. $ENV{GTESTDIR}/include )

set(CMAKE_EXE_LINKER_FLAGS "-L$ENV{DEPDIR}/lib")

add_executable (${PROJECT_NAME} ${SRCS} ${HDRS})

##TARGET_LINK_LIBRARIES(${PROJECT_NAME} fring )

##target_link_libraries (${PROJECT_NAME} gtest boost_filesystem boost_system) 
