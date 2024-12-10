#include <iostream>
#include <unordered_map>
#include <msgpack.hpp>
#include <mariadb/mysql.h>
#include "dataStorage.h"
#include <iomanip>  // For std::put_time
#include <sstream>  // For std::stringstream

// Constructor to initialize the MariaDB connection
DataStorage::DataStorage() {
    conn = mysql_init(NULL);
    if (conn == NULL) {
        std::cerr << "mysql_init() failed" << std::endl;
        return;
    }

    // Connect to the database (replace with your credentials)
    if (mysql_real_connect(conn, "localhost", "my_user", "my_password", "my_database", 0, NULL, 0) == NULL) {
        std::cerr << "mysql_real_connect() failed: " << mysql_error(conn) << std::endl;
        mysql_close(conn);
        return;
    }
}

// Destructor to close the MariaDB connection
DataStorage::~DataStorage() {
    if (conn != NULL) {
        mysql_close(conn);
    }
}

void DataStorage::handleTimestamp(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        // Verify timestamp exists and is of correct type
        if (dataMap.count("timestamp") == 0) {
            throw std::runtime_error("Timestamp key not found");
        }

        // Attempt to extract timestamp
        uint64_t receivedTimestamp = dataMap.at("timestamp").as<uint64_t>();
        
        // Debug print
        std::cout << "Received Raw Timestamp: " << receivedTimestamp << std::endl;

        // Validate timestamp (optional, but can catch some edge cases)
        if (receivedTimestamp == 0) {
            throw std::runtime_error("Zero timestamp received");
        }

        time_t seconds = receivedTimestamp / 1000;                           // Extract seconds
        uint64_t milliseconds = receivedTimestamp % 1000;                    // Extract milliseconds

        // Convert seconds to time structure
        std::tm* tmTime = std::localtime(&seconds);  // Using localtime instead of gmtime
        if (!tmTime) {
            throw std::runtime_error("Failed to parse timestamp");
        }

        // Format timestamp as `YYYY-MM-DD HH:MM:SS.mmm`
        std::stringstream timestampStream;
        timestampStream << std::put_time(tmTime, "%Y-%m-%d %H:%M:%S") << '.' 
                        << std::setw(3) << std::setfill('0') << milliseconds;

        // Store the formatted timestamp
        timestamp.formatted = timestampStream.str();
        std::cout << "Formatted Timestamp: " << timestamp.formatted << std::endl;
    } 
    catch (const std::exception& e) {
        std::cerr << "Timestamp handling error: " << e.what() << std::endl;
        throw;  // Re-throw to prevent silently continuing
    }
}

void DataStorage::createTableIfNotExists() {
    // Ensure tableName is properly set
    if (tableName.empty()) {
        std::cerr << "Error: Table name is empty. Cannot create table." << std::endl;
        throw std::runtime_error("Table name is not defined.");
    }

    // Prepare create table query
    std::stringstream createTableQuery;
    createTableQuery << "CREATE TABLE IF NOT EXISTS `" << tableName << "` ("
                 << "flowRate SMALLINT UNSIGNED NOT NULL, "
                 << "version CHAR(20) NOT NULL, "
                 << "powerReading MEDIUMINT UNSIGNED NOT NULL, "
                 << "frequency MEDIUMINT UNSIGNED NOT NULL, "
                 << "pulseWidth MEDIUMINT UNSIGNED NOT NULL, "
                 << "dcVoltage SMALLINT UNSIGNED NOT NULL, "
                 << "dcCurrent SMALLINT UNSIGNED NOT NULL, "
                 << "channelAForwardVoltage SMALLINT UNSIGNED NOT NULL, "
                 << "channelAReferenceVoltage TINYINT UNSIGNED NOT NULL, "
                 << "channelBForwardVoltage TINYINT UNSIGNED NOT NULL, "
                 << "channelBReferenceVoltage TINYINT UNSIGNED NOT NULL, "
                 << "channelCForwardVoltage TINYINT UNSIGNED NOT NULL, "
                 << "channelCReferenceVoltage TINYINT UNSIGNED NOT NULL, "
                 << "channelDForwardVoltage SMALLINT UNSIGNED NOT NULL, "
                 << "channelDReferenceVoltage TINYINT UNSIGNED NOT NULL, "
                 << "serialNumber TINYINT UNSIGNED NOT NULL, "
                 << "systemType TINYINT UNSIGNED NOT NULL, "
                 << "duty TINYINT UNSIGNED NOT NULL, "
                 << "tubePressure TINYINT UNSIGNED NOT NULL, "
                 << "wavelength TINYINT UNSIGNED NOT NULL, "
                 << "timestamp DATETIME NOT NULL PRIMARY KEY"  // Set timestamp as PRIMARY KEY
                 << ") PARTITION BY RANGE (TO_SECONDS(timestamp)) ("
                 << " PARTITION p0 VALUES LESS THAN (TO_SECONDS('2024-01-01 00:00:00'))"
                 << ");";

    std::string queryStr = createTableQuery.str();

    // Debug: Print the query
    std::cout << "Generated Query: " << queryStr << std::endl;

    // Execute query
    if (mysql_query(conn, queryStr.c_str())) {
        std::cerr << "Table creation failed: " << mysql_error(conn) << std::endl;
        throw std::runtime_error("Could not create table");
    }
}


void DataStorage::addPartitionsForNextHour() {
    // Get current time
    std::time_t now = std::time(nullptr);
    std::tm* currentTime = std::localtime(&now);

    // Prepare ALTER TABLE query
    std::stringstream alterTableQuery;
    alterTableQuery << "ALTER TABLE `laser_data` ADD PARTITION (";

    for (int minute = 0; minute < 60; ++minute) {
        std::tm partitionTime = *currentTime;
        partitionTime.tm_min += minute;  // Add minutes incrementally
        mktime(&partitionTime);          // Normalize the time structure

        // Format the partition name and range
        alterTableQuery << "PARTITION p"
                        << partitionTime.tm_hour
                        << std::setw(2) << std::setfill('0') << partitionTime.tm_min
                        << " VALUES LESS THAN (TO_SECONDS('"
                        << (partitionTime.tm_year + 1900) << "-"
                        << std::setw(2) << std::setfill('0') << (partitionTime.tm_mon + 1) << "-"
                        << std::setw(2) << std::setfill('0') << partitionTime.tm_mday << " "
                        << std::setw(2) << std::setfill('0') << partitionTime.tm_hour << ":"
                        << std::setw(2) << std::setfill('0') << partitionTime.tm_min << ":00'))";
        if (minute < 59) {
            alterTableQuery << ", ";
        }
    }
    alterTableQuery << ");";

    // Execute the ALTER TABLE query
    std::string queryStr = alterTableQuery.str();
    std::cout << "Generated Query: " << queryStr << std::endl;

    if (mysql_query(conn, queryStr.c_str())) {
        std::cerr << "Failed to add partitions: " << mysql_error(conn) << std::endl;
    } else {
        std::cout << "Partitions for the next hour added successfully." << std::endl;
    }
}




void DataStorage::insertAllData() {
    // Ensure table exists before inserting
    createTableIfNotExists();
    // Check and create monthly partition if needed
    addPartitionsForNextHour();

    

    std::stringstream queryStream;
    queryStream << "INSERT INTO `" << tableName << "` (flowRate, version, powerReading, frequency, pulseWidth, dcVoltage, dcCurrent, channelAForwardVoltage, channelAReferenceVoltage, channelBForwardVoltage, channelBReferenceVoltage, channelCForwardVoltage, channelCReferenceVoltage, channelDForwardVoltage, channelDReferenceVoltage, serialNumber, systemType, duty, tubePressure, wavelength, timestamp) VALUES (";
    
    queryStream << laserheadFlow.flowRate << ", "           // flow rate (SMALLINT UNSIGNED)
                << "'" << version.version << "', "          // Fixed-length version string
                << power.powerReading << ", "               // power reading (MEDIUMINT UNSIGNED)
                << pwmModulation.frequency << ", "          // frequency (MEDIUMINT UNSIGNED)
                << pwmModulation.pulseWidth << ", "         // pulse width (MEDIUMINT UNSIGNED)
                << dcInfo.dcVoltage << ", "                 // DC voltage (SMALLINT UNSIGNED)
                << dcInfo.dcCurrent << ", "                 // DC current (SMALLINT UNSIGNED)
                << rfInfo.channelAForwardVoltage << ", "    // Channel A forward voltage (SMALLINT UNSIGNED)
                << rfInfo.channelAReferenceVoltage << ", "  // Channel A reference voltage (TINYINT UNSIGNED)
                << rfInfo.channelBForwardVoltage << ", "    // Channel B forward voltage (TINYINT UNSIGNED)
                << rfInfo.channelBReferenceVoltage << ", "  // Channel B reference voltage (TINYINT UNSIGNED)
                << rfInfo.channelCForwardVoltage << ", "    // Channel C forward voltage (TINYINT UNSIGNED)
                << rfInfo.channelCReferenceVoltage << ", "  // Channel C reference voltage (TINYINT UNSIGNED)
                << rfInfo.channelDForwardVoltage << ", "    // Channel D forward voltage (TINYINT UNSIGNED)
                << rfInfo.channelDReferenceVoltage << ", "  // Channel D reference voltage (TINYINT UNSIGNED)
                << systemInfo.serialNumber << ", "          // serial number (TINYINT UNSIGNED)
                << systemInfo.systemType << ", "            // system type (TINYINT UNSIGNED)
                << systemInfo.duty << ", "                  // duty (TINYINT UNSIGNED)
                << systemInfo.tubePressure << ", "          // tube pressure (TINYINT UNSIGNED)
                << systemInfo.wavelength << ", "            // wavelength (TINYINT UNSIGNED)
                << "'" << timestamp.formatted << "')";      // Timestamp with millisecond precision

    std::string query = queryStream.str();

    std::cout << "Generated Query: " << query << std::endl;
    
    if (mysql_query(conn, query.c_str())) {
        std::cerr << "INSERT failed: " << mysql_error(conn) << std::endl;
        std::cerr << "Query: " << query << std::endl;
    }
}

void DataStorage::handleLaserheadFlow(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        laserheadFlow.flowRate = dataMap.at("flowRate").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing flowRate key
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for flowRate
    }
    handleTimestamp(dataMap);
}

void DataStorage::handleVersion(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        version.version = dataMap.at("version").as<std::string>();
    } catch (const std::out_of_range&) {
        // Handle missing version key
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for version
    }
    handleTimestamp(dataMap);
}

void DataStorage::handlePower(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        power.powerReading = dataMap.at("powerReading").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing power key
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for power
    }
    handleTimestamp(dataMap);
}

void DataStorage::handlePWMModulation(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        pwmModulation.frequency = dataMap.at("frequency").as<uint32_t>();
        pwmModulation.pulseWidth = dataMap.at("pulseWidth").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing one or more PWM Modulation keys
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for PWM Modulation data
    }
    handleTimestamp(dataMap);
}

void DataStorage::handleDcInfo(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        dcInfo.dcVoltage = dataMap.at("dcVoltage").as<uint32_t>();
        dcInfo.dcCurrent = dataMap.at("dcCurrent").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing one or more DC Info keys
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for DC Info data
    }
    handleTimestamp(dataMap);
}

void DataStorage::handleRfInfo(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        rfInfo.channelAForwardVoltage = dataMap.at("channelAForwardVoltage").as<uint32_t>();
        rfInfo.channelAReferenceVoltage = dataMap.at("channelAReferenceVoltage").as<uint32_t>();
        rfInfo.channelBForwardVoltage = dataMap.at("channelBForwardVoltage").as<uint32_t>();
        rfInfo.channelBReferenceVoltage = dataMap.at("channelBReferenceVoltage").as<uint32_t>();
        rfInfo.channelCForwardVoltage = dataMap.at("channelCForwardVoltage").as<uint32_t>();
        rfInfo.channelCReferenceVoltage = dataMap.at("channelCReferenceVoltage").as<uint32_t>();
        rfInfo.channelDForwardVoltage = dataMap.at("channelDForwardVoltage").as<uint32_t>();
        rfInfo.channelDReferenceVoltage = dataMap.at("channelDReferenceVoltage").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing RF Info keys
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for RF Info data
    }
    handleTimestamp(dataMap);
}

void DataStorage::handleSystemInfo(const std::unordered_map<std::string, msgpack::object>& dataMap) {
    try {
        systemInfo.serialNumber = dataMap.at("serialNumber").as<uint32_t>();
        systemInfo.systemType = dataMap.at("systemType").as<uint32_t>();
        systemInfo.duty = dataMap.at("duty").as<uint32_t>();
        systemInfo.tubePressure = dataMap.at("tubePressure").as<uint32_t>();
        systemInfo.wavelength = dataMap.at("wavelength").as<uint32_t>();
    } catch (const std::out_of_range&) {
        // Handle missing systemStatus key
    } catch (const msgpack::type_error&) {
        // Handle type mismatch for systemStatus
    }
    handleTimestamp(dataMap);
}