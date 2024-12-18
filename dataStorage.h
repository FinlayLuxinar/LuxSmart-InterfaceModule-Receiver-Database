#ifndef DATASTORAGE_H
#define DATASTORAGE_H

#include <string>
#include <unordered_map>
#include <msgpack.hpp>
#include <mariadb/mysql.h>

class DataStorage {
public:

    DataStorage();
    ~DataStorage();
    
    struct LaserheadFlow {
        uint32_t flowRate;
    };
    
    struct Version {
        std::string version;
    };

    struct Power {
        uint32_t powerReading;
    };

    struct PWMModulation {
        uint32_t frequency;
        uint32_t pulseWidth;
    };


    struct DCInfo {
        uint32_t dcVoltage;
        uint32_t dcCurrent;
    };

    struct RFInfo {
        uint32_t channelAForwardVoltage;   ///< Forward voltage for Channel A
        uint32_t channelAReferenceVoltage;  ///< Reference voltage for Channel A
        uint32_t channelBForwardVoltage;   ///< Forward voltage for Channel B
        uint32_t channelBReferenceVoltage;  ///< Reference voltage for Channel B
        uint32_t channelCForwardVoltage;   ///< Forward voltage for Channel C
        uint32_t channelCReferenceVoltage;  ///< Reference voltage for Channel C
        uint32_t channelDForwardVoltage;   ///< Forward voltage for Channel D
        uint32_t channelDReferenceVoltage;  ///< Reference voltage for Channel D
    };

    struct systemInfo {
        uint32_t serialNumber;
        uint32_t systemType;
        uint32_t duty;
        uint32_t tubePressure;
        uint32_t wavelength;
    };

struct Timestamp {
    std::time_t value;
    std::string formatted;
};


    std::string tableName = "laser_data";
    LaserheadFlow laserheadFlow;
    Version version;
    Power power;
    PWMModulation pwmModulation;
    DCInfo dcInfo;
    RFInfo rfInfo;
    systemInfo systemInfo;
    Timestamp timestamp;

    void handleLaserheadFlow(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handleVersion(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handlePower(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handlePWMModulation(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handleDcInfo(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handleRfInfo(const std::unordered_map<std::string, msgpack::object>& dataMap);
    void handleSystemInfo(const std::unordered_map<std::string, msgpack::object>& dataMap);

    void handleTimestamp(const std::unordered_map<std::string, msgpack::object>& dataMap);

    void insertAllData();
    void checkAndCreateMonthlyPartitions();
    std::string getCurrentPartitionName();

    double GetMaxStorage();
    

    // Database connection
    MYSQL* conn;
};




#endif // DATASTORAGE_H
    

    

    

    

    