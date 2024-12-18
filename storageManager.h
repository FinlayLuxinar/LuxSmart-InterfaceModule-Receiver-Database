#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H

#include <string>
#include <vector>
#include <mariadb/mysql.h>

class StorageManager {
public:
    StorageManager(const std::string& dbHost, const std::string& dbUser, const std::string& dbPass, const std::string& dbName);
    ~StorageManager();

    // Main method to handle data reduction
    void reduceStorage(const std::string& outputFolder);

    bool checkDiskUsage();
    double GetMaxStorage();

private:
    MYSQL* conn; // Database connection

    // Helper methods
    std::vector<std::string> getOldestPartitions(int count);
    std::vector<std::string> getFoldersInDirectory(const std::string& directory);
    void exportPartitionToCSV(const std::string& partitionName, const std::string& outputFolder);
    void deletePartition(const std::string& partitionName);
    
    void connect();
    void disconnect();
    int GetDeletionAmount();
};

void createDirectory(const std::string& path);

#endif
