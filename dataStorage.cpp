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

void DataStorage::insertAllData() {
    std::stringstream queryStream;
    queryStream << "INSERT INTO data (flowRate, version, powerReading, frequency, pulseWidth, dcVoltage, dcCurrent, channelAForwardVoltage, channelAReferenceVoltage, channelBForwardVoltage, channelBReferenceVoltage, channelCForwardVoltage, channelCReferenceVoltage, channelDForwardVoltage, channelDReferenceVoltage, serialNumber, systemType, duty, tubePressure, waveLength, timestamp) VALUES (";
    
    queryStream << laserheadFlow.flowRate << ", "           // flow rate
                << "'" << version.version << "', "          // Properly quoted string
                << power.powerReading << ", "               // power reading
                << pwmModulation.frequency << ", "          // frequency
                << pwmModulation.pulseWidth << ", "         // pulse width
                << dcInfo.dcVoltage << ", "                 // DC voltage
                << dcInfo.dcCurrent << ", "                 // DC current
                << rfInfo.channelAForwardVoltage << ", "    // Channel A forward voltage
                << rfInfo.channelAReferenceVoltage << ", "  // Channel A reference voltage
                << rfInfo.channelBForwardVoltage << ", "    // Channel B forward voltage
                << rfInfo.channelBReferenceVoltage << ", "  // Channel B reference voltage
                << rfInfo.channelCForwardVoltage << ", "    // Channel C forward voltage
                << rfInfo.channelCReferenceVoltage << ", "  // Channel C reference voltage
                << rfInfo.channelDForwardVoltage << ", "    // Channel D forward voltage
                << rfInfo.channelDReferenceVoltage << ", "  // Channel D reference voltage
                << systemInfo.serialNumber << ", "          // serial number
                << systemInfo.systemType << ", "            // system type
                << systemInfo.duty << ", "                  // duty
                << systemInfo.tubePressure << ", "          // tube pressure
                << systemInfo.wavelength << ", "            // wavelength
                << "'" << timestamp.formatted << "')";      // Enclosed in single quotes

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