cmake_minimum_required(VERSION 3.10)

set(CMAKE_BUILD_TYPE Debug)

project(ShnoDatabase)

add_subdirectory(dependencies/fmt EXCLUDE_FROM_ALL)
add_subdirectory(dependencies/json EXCLUDE_FROM_ALL)

add_executable(main main.cpp myrandom.cpp myrandom.hpp mytree.cpp mytree.hpp database.cpp database.hpp)
target_compile_features(main PUBLIC cxx_std_17)
target_link_libraries(main fmt-header-only nlohmann_json)