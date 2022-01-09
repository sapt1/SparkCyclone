cmake_minimum_required(VERSION 3.6)
project(cyclone-host LANGUAGES CXX C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set (CMAKE_CXX_STANDARD 17)

include_directories(.)
include_directories(frovedis/text/)
include_directories(frovedis/core/)
include_directories(frovedis/dataframe/)

add_library(cyclone-host SHARED cyclone.cc frovedis/text/words.cc)
if(WIN32)
  target_link_libraries(cyclone-host)
endif()
