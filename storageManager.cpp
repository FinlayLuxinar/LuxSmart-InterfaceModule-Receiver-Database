#include "storageManager.h"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <sys/statvfs.h> // For disk space checking
#include <ctime>
#include <algorithm>
#include <filesystem>
#include <sys/stat.h>
#include <dirent.h>

// Constructor: Initializes database connection
StorageManager::StorageManager(const std::string& dbHost, const std::string& dbUser, const std::string& dbPass, const std::string& dbName) {
    conn = mysql_init(nullptr);
    if (!conn) {
        throw std::runtime_error("MySQL initialization failed");
    }

    if (!mysql_real_connect(conn, dbHost.c_str(), dbUser.c_str(), dbPass.c_str(), dbName.c_str(), 0, nullptr, 0)) {
        throw std::runtime_error("MySQL connection failed: " + std::string(mysql_error(conn)));
    }
}

// Destructor: Closes database connection
StorageManager::~StorageManager() {
    disconnect();
}

// Connect to the database
void StorageManager::connect() {
    if (!conn) {
        throw std::runtime_error("Database connection is not initialized");
    }
}

// Disconnect from the database
void StorageManager::disconnect() {
    if (conn) {
        mysql_close(conn);
        conn = nullptr;
    }
}

// Get the names of the oldest partitions
std::vector<std::string> StorageManager::getOldestPartitions(int count) {
    std::vector<std::string> partitions;
    
    // Get the current date
    std::time_t now = std::time(nullptr);
    std::tm* currentTime = std::localtime(&now);
    
    // Create a date string for the current Day
    char currentDayStr[20];
    std::strftime(currentDayStr, sizeof(currentDayStr), "%Y%m", currentTime);

    std::string query = "SELECT partition_name FROM information_schema.partitions " 
                        "WHERE table_name = 'laser_data' " 
                        "AND partition_name NOT LIKE 'p" + std::string(currentDayStr) + "%' " 
                        "ORDER BY partition_ordinal_position ASC LIMIT " + std::to_string(count) + ";";

    if (mysql_query(conn, query.c_str())) {
        throw std::runtime_error("Failed to fetch partitions: " + std::string(mysql_error(conn)));
    }

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        throw std::runtime_error("Failed to store result: " + std::string(mysql_error(conn)));
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        partitions.push_back(row[0]);
    }

    mysql_free_result(result);
    return partitions;
}

// ----------------------------------------------------------------------------------------

bool StorageManager::checkDiskUsage() {
    struct statvfs stat;
    double threshold = GetMaxStorage();

    // Get file system statistics
    if (statvfs("/", &stat) != 0) {
        std::cerr << "Failed to get disk space information" << std::endl;
        return false;
    }

    // Calculate disk usage as a percentage
    double freeSpace = stat.f_bfree * stat.f_frsize;
    double totalSpace = stat.f_blocks * stat.f_frsize;
    double usedSpace = totalSpace - freeSpace;
    double usagePercentage = (usedSpace / totalSpace) * 100.0;

    std::cout << "Disk usage: " << usagePercentage << "%" << std::endl;

    return usagePercentage > threshold;  // Returns true if disk usage exceeds threshold
}

// Function to get the maximum allowed disk storage threshold from the database
double StorageManager::GetMaxStorage() {
    std::string getDiskUsageQuery = "SELECT maxStorage FROM settings WHERE id = 1;";
    if (mysql_query(conn, getDiskUsageQuery.c_str())) {
        std::cerr << "SELECT failed: " << mysql_error(conn) << std::endl;
        return 0.0;  // Return a default value in case of an error
    }

    // Get the result
    MYSQL_RES* result = mysql_store_result(conn);
    if (result == NULL) {
        std::cerr << "Failed to store result from SELECT query" << std::endl;
        return 0.0;  // Return a default value in case of an error
    }

    // Get the value from the result
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
        std::cerr << "Failed to fetch row from result" << std::endl;
        mysql_free_result(result);
        return 0.0;  // Return a default value in case of an error
    }

    // Convert the value to a double
    double diskUsage = std::stod(row[0]);

    // Free the result
    mysql_free_result(result);

    std::cout << "Max Storage: " << diskUsage << "%" << std::endl;
    return diskUsage;
}

// Function to get the maximum allowed disk storage threshold from the database
int StorageManager::GetDeletionAmount() {
    std::string getDeletionAmountQuery = "SELECT MonthsToRemove FROM settings WHERE id = 1;";
    if (mysql_query(conn, getDeletionAmountQuery.c_str())) {
        std::cerr << "SELECT failed: " << mysql_error(conn) << std::endl;
        return 0;  // Return a default value in case of an error
    }

    // Get the result
    MYSQL_RES* result = mysql_store_result(conn);
    if (result == NULL) {
        std::cerr << "Failed to store result from SELECT query" << std::endl;
        return 0;  // Return a default value in case of an error
    }

    // Get the value from the result
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
        std::cerr << "Failed to fetch row from result" << std::endl;
        mysql_free_result(result);
        return 0;  // Return a default value in case of an error
    }

    // Convert the value to an integer
    int deletionAmount = std::stoi(row[0]);

    // Free the result
    mysql_free_result(result);

    std::cout << "Deletion Amount: " << deletionAmount << " Days" << std::endl;
    return deletionAmount;
}

// ----------------------------------------------------------------------------------------

// Export a partition's data to a CSV file
void StorageManager::exportPartitionToCSV(const std::string& partitionName, const std::string& outputFolder) {
    // Extract the year and Day from the partition name (e.g., p20241001 -> 202410)
    std::string partitionDay = partitionName.substr(1, 6);  // Skip the 'p' and get 'YYYYMM'

    // Create the folder path using the partition's year and Day
    std::string directoryPath = outputFolder + "/" + partitionDay;

    // Create the directory if it doesn't exist
    createDirectory(directoryPath);

    // Prepare the query to fetch data from the partition
    std::string query = "SELECT * FROM laser_data PARTITION (" + partitionName + ");";
    if (mysql_query(conn, query.c_str())) {
        throw std::runtime_error("Failed to fetch partition data: " + std::string(mysql_error(conn)));
    }

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        throw std::runtime_error("Failed to store result: " + std::string(mysql_error(conn)));
    }

    // Set the output file path
    std::string outputFile = directoryPath + "/" + partitionName + ".csv";
    std::ofstream csvFile(outputFile);
    if (!csvFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + outputFile);
    }

    // Write headers
    MYSQL_ROW row;
    MYSQL_FIELD* fields = mysql_fetch_fields(result);
    int numFields = mysql_num_fields(result);

    for (int i = 0; i < numFields; ++i) {
        csvFile << fields[i].name << (i < numFields - 1 ? "," : "\n");
    }

    // Write rows
    while ((row = mysql_fetch_row(result))) {
        for (int i = 0; i < numFields; ++i) {
            csvFile << (row[i] ? row[i] : "") << (i < numFields - 1 ? "," : "\n");
        }
    }

    csvFile.close();
    mysql_free_result(result);
}


void createDirectory(const std::string& path) {
    // Create the directory if it doesn't exist
    if (mkdir(path.c_str(), 0777) == -1) {
        if (errno != EEXIST) {
            throw std::runtime_error("Failed to create directory: " + path);
        }
    }
}

// Delete a partition from the database
void StorageManager::deletePartition(const std::string& partitionName) {
    std::string query = "ALTER TABLE laser_data DROP PARTITION " + partitionName + ";";
    if (mysql_query(conn, query.c_str())) {
        throw std::runtime_error("Failed to delete partition: " + std::string(mysql_error(conn)));
    }
}

void StorageManager::reduceStorage(const std::string& outputFolder) {
    if (checkDiskUsage()) {
        int daysToDelete = GetDeletionAmount();
        std::vector<std::string> partitions = getOldestPartitions(daysToDelete);

        // Additional safety check
        std::time_t now = std::time(nullptr);
        std::tm* currentTime = std::localtime(&now);
        char currentDayStr[20];
        std::strftime(currentDayStr, sizeof(currentDayStr), "%Y%m", currentTime);

        // Filter out any partitions from the current Day
        partitions.erase(
            std::remove_if(partitions.begin(), partitions.end(),
                [&currentDayStr](const std::string& partition) {
                    return partition.substr(1, 6) == currentDayStr;
                }),
            partitions.end()
        );

        if (partitions.empty()) {
            std::cout << "No partitions found for deletion." << std::endl;
            return;
        }

        std::string currentFolder;
        bool folderHasData = false;

        // Process partitions
        for (const auto& partition : partitions) {
            std::cout << "Exporting partition: " << partition << std::endl;
            exportPartitionToCSV(partition, outputFolder);

            // Delete the partition from the database after exporting
            deletePartition(partition);

            // Check if we're moving from one folder to the next (i.e., move to a new Day folder)
            std::string partitionDay = partition.substr(1, 6);  // '202410' from 'p20241001'
            if (currentFolder != partitionDay) {
                // Compress the current folder if data was added and the folder is not empty
                if (!currentFolder.empty() && folderHasData) {
                    std::cout << "Compressing folder: " << currentFolder << std::endl;
                    std::string zipCommand = "zip -rj " + outputFolder + "/" + currentFolder + ".zip " + outputFolder + "/" + currentFolder;
                    int result = system(zipCommand.c_str());  // Use system to run the zip command
                    if (result != 0) {
                        std::cerr << "Error compressing folder: " << currentFolder << std::endl;
                    }
                }

                // Reset for the new folder
                currentFolder = partitionDay;
                folderHasData = true;
            }
        }

        // Check the number of partition folders in the directory
        std::vector<std::string> folders = getFoldersInDirectory(outputFolder);

        // If more than one folder exists, compress the oldest folders
        if (folders.size() > 1) {
            std::cout << "More than one folder found, compressing oldest folders..." << std::endl;
            std::sort(folders.begin(), folders.end());  // Sort to get the oldest first

            // Compress all but the most recent folder
            for (size_t i = 0; i < folders.size() - 1; ++i) {
                std::cout << "Compressing folder: " << folders[i] << std::endl;
                std::string zipCommand = "zip -r " + outputFolder + "/" + folders[i] + ".zip " + outputFolder + "/" + folders[i];
                int result = system(zipCommand.c_str());
                if (result != 0) {
                    std::cerr << "Error compressing folder: " << folders[i] << std::endl;
                }
            }
        }
    } else {
        std::cout << "Disk usage is below the threshold, no action required." << std::endl;
    }
}


std::vector<std::string> StorageManager::getFoldersInDirectory(const std::string& directory) {
    std::vector<std::string> folders;

    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr) {
        std::cerr << "Error opening directory " << directory << std::endl;
        return folders;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // Only consider directories that match the yyyymm format
        if (entry->d_type == DT_DIR) {
            std::string folderName = entry->d_name;
            if (folderName.size() == 6 && std::isdigit(folderName[0]) && std::isdigit(folderName[1]) &&
                std::isdigit(folderName[2]) && std::isdigit(folderName[3]) &&
                std::isdigit(folderName[4]) && std::isdigit(folderName[5])) {
                folders.push_back(folderName);
            }
        }
    }
    closedir(dir);

    return folders;
}