cmake_minimum_required(VERSION 3.10)
project(mqttReceiver VERSION 0.7.7)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# Specify the source files
set(SOURCES
    main.cpp
    dataStorage.cpp
    receiver.cpp
    storageManager.cpp
)

# Add the executable target
add_executable(subMQTT ${SOURCES})

# Specify include directories
target_include_directories(subMQTT PRIVATE ${CMAKE_SOURCE_DIR})

# Set the Paho MQTT C++ directory
set(PahoMqttCpp_DIR "/usr/lib/aarch64-linux-gnu/cmake/eclipse-paho-mqtt-c")

# Find required packages
find_package(PahoMqttCpp REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

# Use pkg-config to find ZeroMQ, UUID, and MariaDB
pkg_check_modules(ZMQ REQUIRED libzmq)
pkg_check_modules(MARIADB REQUIRED libmariadb)

# Add include directories for external libraries
target_include_directories(subMQTT PRIVATE
    ${PAHO_MQTT_CPP_INCLUDE_DIRS}
    ${ZMQ_INCLUDE_DIRS}
    ${MARIADB_INCLUDE_DIRS}
)

# Link the libraries to the target
target_link_libraries(subMQTT PRIVATE
    PahoMqttCpp::paho-mqttpp3
    nlohmann_json::nlohmann_json
    ${ZMQ_LIBRARIES}
    Threads::Threads
    ${MARIADB_LIBRARIES}  # Link MariaDB client library
)

# Debug information
message(STATUS "Paho MQTT include directories: ${PAHO_MQTT_CPP_INCLUDE_DIRS}")
message(STATUS "ZeroMQ include directories: ${ZMQ_INCLUDE_DIRS}")
message(STATUS "ZeroMQ libraries: ${ZMQ_LIBRARIES}")
message(STATUS "MariaDB include directories: ${MARIADB_INCLUDE_DIRS}")
message(STATUS "MariaDB libraries: ${MARIADB_LIBRARIES}")

# CPack configuration
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_NAME "mqttReceiver")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_CONTACT "Finlay Allen <finlay.allen@luxinar.com>")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Finlay Allen <finlay.allen@luxinar.com>")
set(CPACK_PACKAGE_DESCRIPTION "My Project Description")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libpaho-mqttpp-dev, nlohmann-json3-dev, libzmq3-dev, libmariadb-dev, libc6 (>= 2.28), libstdc++6 (>= 6), uuid-dev")
set(CPACK_DEBIAN_ARCHITECTURE "arm64")
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

include(CPack)

# Installation configuration
install(TARGETS subMQTT DESTINATION /usr/local/bin)
