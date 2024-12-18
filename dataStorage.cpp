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
    } 
    catch (const std::exception& e) {
        std::cerr << "Timestamp handling error: " << e.what() << std::endl;
        throw;  // Re-throw to prevent silently continuing
    }
}

void createMonthlyPartitions(MYSQL* conn) {
    // Get current time
    std::time_t now = std::time(nullptr);
    std::tm* currentTime = std::localtime(&now);
    
    // Determine days in current month
    int daysInMonth = [](int year, int month) {
        static const int days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        return (month == 1 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) ? 29 : days[month];
    }(currentTime->tm_year + 1900, currentTime->tm_mon);

    // Existing partitions tracking
    std::map<std::string, long long> existingPartitions;
    std::string checkQuery = 
        "SELECT partition_name, CAST(partition_description AS SIGNED) as part_desc FROM information_schema.partitions "
        "WHERE table_schema = DATABASE() AND table_name = 'laser_data' "
        "ORDER BY part_desc";

    if (mysql_query(conn, checkQuery.c_str())) {
        std::cerr << "Failed to check partitions: " << mysql_error(conn) << std::endl;
        return;
    }

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        std::cerr << "Failed to retrieve partitions: " << mysql_error(conn) << std::endl;
        return;
    }

    long long lastPartitionValue = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        std::string partitionName(row[0]);
        long long partitionValue = row[1] ? std::stoll(row[1]) : 0;
        existingPartitions[partitionName] = partitionValue;
        lastPartitionValue = std::max(lastPartitionValue, partitionValue);
    }
    mysql_free_result(result);

    // Prepare partition creation query
    std::stringstream partitionQuery;
    partitionQuery << "ALTER TABLE laser_data ADD PARTITION (";

    bool firstPartition = true;
    bool partitionNeedsAdding = false;

    // Create partitions for remaining days
    for (int day = currentTime->tm_mday; day <= daysInMonth; ++day) {
        // Construct partition time
        std::tm partitionTime = *currentTime;
        partitionTime.tm_mday = day;
        partitionTime.tm_hour = 0;
        partitionTime.tm_min = 0;
        partitionTime.tm_sec = 0;
        std::mktime(&partitionTime);

        // Generate partition name (PYYYYMMDD)
        char partitionName[20];
        std::snprintf(partitionName, sizeof(partitionName), 
            "P%d%02d%02d", 
            partitionTime.tm_year + 1900, 
            partitionTime.tm_mon + 1, 
            partitionTime.tm_mday);

        // Skip existing partitions
        if (existingPartitions.count(partitionName) > 0) {
            std::cout << "Partition " << partitionName << " already exists. Skipping." << std::endl;
            continue;
        }

        // Format date for VALUES LESS THAN ('YYYY-MM-DD 00:00:00')
        char dateBuffer[80];
        std::strftime(dateBuffer, sizeof(dateBuffer), "%Y-%m-%d 00:00:00", &partitionTime);

        // Add to partition query in the correct format
        if (!firstPartition) {
            partitionQuery << ", ";
        }
        partitionQuery << "PARTITION " << partitionName 
                       << " VALUES LESS THAN ('" << dateBuffer << "')";

        firstPartition = false;
        partitionNeedsAdding = true;
    }

    partitionQuery << ");";

    // Execute partition creation if needed
    if (!partitionNeedsAdding) {
        std::cout << "No new partitions to add." << std::endl;
        return;
    }

    std::string queryStr = partitionQuery.str();
    std::cout << "Generated Query: " << queryStr << std::endl;

    if (mysql_query(conn, queryStr.c_str())) {
        std::cerr << "Failed to add partitions: " << mysql_error(conn) << std::endl;
    } else {
        std::cout << "Partitions added successfully." << std::endl;
    }
}


std::string DataStorage::getCurrentPartitionName() {
    // Get current time
    std::time_t now = std::time(nullptr);
    std::tm* currentTime = std::localtime(&now);

    // Format partition name as pYYYYMMDD
    char partitionName[20];
    std::snprintf(partitionName, sizeof(partitionName), 
        "p%d%02d%02d", 
        currentTime->tm_year + 1900,  // Years since 1900 
        currentTime->tm_mon + 1,      // Months are 0-11, so add 1 
        currentTime->tm_mday);        // Day of the month

    return std::string(partitionName);
}

// Function to insert all data into the database
void DataStorage::insertAllData() {

    // Check and create monthly partition if needed
    createMonthlyPartitions(conn);

    

   std::string currentPartition = getCurrentPartitionName();

    std::stringstream queryStream;
    queryStream << "INSERT INTO `" << tableName << "` ("
            << "flowRate, version, powerReading, frequency, pulseWidth, "
            << "dcVoltage, dcCurrent, channelAForwardVoltage, channelAReferenceVoltage, "
            << "channelBForwardVoltage, channelBReferenceVoltage, "
            << "channelCForwardVoltage, channelCReferenceVoltage, "
            << "channelDForwardVoltage, channelDReferenceVoltage, "
            << "serialNumber, systemType, duty, tubePressure, wavelength, "
            << "timestamp) VALUES (";

    
    queryStream << laserheadFlow.flowRate << ", " 
                << "'" << version.version << "', "        
                << power.powerReading << ", "               
                << pwmModulation.frequency << ", "          
                << pwmModulation.pulseWidth << ", "         
                << dcInfo.dcVoltage << ", "                 
                << dcInfo.dcCurrent << ", "                 
                << rfInfo.channelAForwardVoltage << ", "    
                << rfInfo.channelAReferenceVoltage << ", "  
                << rfInfo.channelBForwardVoltage << ", "    
                << rfInfo.channelBReferenceVoltage << ", "  
                << rfInfo.channelCForwardVoltage << ", "    
                << rfInfo.channelCReferenceVoltage << ", "  
                << rfInfo.channelDForwardVoltage << ", "    
                << rfInfo.channelDReferenceVoltage << ", "  
                << systemInfo.serialNumber << ", "          
                << systemInfo.systemType << ", "            
                << systemInfo.duty << ", "                  
                << systemInfo.tubePressure << ", "          
                << systemInfo.wavelength << ", "            
                << "'" << timestamp.formatted << "')";      

    std::string query = queryStream.str();

    std::cout << "Generated Query: " << query << std::endl;
    
    if (mysql_query(conn, query.c_str())) {
        std::cerr << "INSERT failed: " << mysql_error(conn) 
                  << "\nQuery: " << query << std::endl;
        
        // Additional diagnostic information
        std::cout << "Debug Info:" << std::endl;
        std::cout << "Timestamp: " << timestamp.formatted << std::endl;
        std::cout << "Partition: " << currentPartition << std::endl;
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