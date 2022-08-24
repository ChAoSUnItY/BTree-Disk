cmake_minimum_required(VERSION 3.10)

set(CMAKE_BUILD_TYPE Debug)

project(ShnoDatabase)

add_subdirectory(dependencies/fmt EXCLUDE_FROM_ALL)
add_subdirectory(dependencies/json EXCLUDE_FROM_ALL)

set(CMAKE_BUILD_TYPE Debug)
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -g")

add_executable(main main.cpp myrandom.cpp myrandom.hpp mytree.cpp mytree.hpp database.cpp database.hpp command.cpp command.hpp common.hpp)
target_compile_features(main PUBLIC cxx_std_20)
target_link_libraries(main fmt-header-only nlohmann_json)

add_executable(test_main test_main.cpp myrandom.cpp myrandom.hpp mytree.cpp mytree.hpp database.cpp database.hpp command.cpp command.hpp common.hpp)
target_compile_features(test_main PUBLIC cxx_std_20)
target_link_libraries(test_main fmt-header-only nlohmann_json)