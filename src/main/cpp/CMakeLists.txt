cmake_minimum_required(VERSION 3.6)
project(cyclone-host LANGUAGES CXX C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set (CMAKE_CXX_STANDARD 17)

include_directories(./)
include_directories(frovedis/text/)
include_directories(frovedis/core/)
include_directories(frovedis/dataframe/)


if(WIN32)
  add_library(cyclone-host SHARED cyclone.cc frovedis/core/utility.cc frovedis/dataframe/join.cc frovedis/text/char_int_conv.cc frovedis/text/datetime_to_words.cc frovedis/text/dict.cc frovedis/text/find.cc frovedis/text/float_to_words.cc frovedis/text/int_to_words.cc frovedis/text/parsedatetime.cc frovedis/text/parsefloat.cc frovedis/text/words.cc)
  target_link_libraries(cyclone-host)
else()
    add_library(cyclone-host SHARED cyclone.cc)
endif()
