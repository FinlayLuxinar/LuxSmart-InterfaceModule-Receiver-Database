#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <mariadb/mysql.h>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

void executeQuery(MYSQL *conn, const std::string& query, MYSQL_RES** result) {
    if (mysql_query(conn, query.c_str())) {
        std::cerr << "Error executing query: " << mysql_error(conn) << std::endl;
        exit(1);
    }
    *result = mysql_store_result(conn);
}

std::pair<int, int> getOldestPartition(MYSQL *conn) {
    // Query to get partition names
    std::string query = "SELECT partition_name, partition_description "
                        "FROM information_schema.partitions "
                        "WHERE table_schema = 'my_database' AND table_name = 'data_partitioned' "
                        "AND partition_method = 'RANGE'";
    MYSQL_RES *result;
    executeQuery(conn, query, &result);

    // Vector to hold (year, month) pairs
    std::vector<std::pair<int, int>> partitionDates;

    // Parse partition names
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        std::string partitionName = row[0];
        std::string partitionDesc = row[1];

        // Skip 'future' partition
        if (partitionName == "future") continue;

        // Extract year and month from partition name (assuming format like p202312)
        if (partitionName.length() == 7 && partitionName[0] == 'p') {
            int year = std::stoi(partitionName.substr(1, 4));
            int month = std::stoi(partitionName.substr(5, 2));
            partitionDates.push_back({year, month});
        }
    }

    mysql_free_result(result);

    // Find the oldest (earliest) date
    if (partitionDates.empty()) {
        std::cerr << "No partitions found." << std::endl;
        exit(1);
    }

    auto oldest = partitionDates[0];
    for (const auto& date : partitionDates) {
        if (date.first < oldest.first || (date.first == oldest.first && date.second < oldest.second)) {
            oldest = date;
        }
    }

    return oldest;
}

void dumpPartitionToCSV(MYSQL *conn, const std::pair<int, int>& partition) {
    // Create a unique folder for the current month
    std::string monthStr = (partition.second < 10 ? "0" : "") + 
                            std::to_string(partition.second) + "_" + 
                            std::to_string(partition.first % 100);
    std::string folderName = "data_" + monthStr;
    fs::create_directory(folderName);

    // Prepare CSV export query for this partition
    std::stringstream exportQuery;
    exportQuery << "SELECT * FROM data PARTITION(p" 
                << partition.first 
                << std::setw(2) << std::setfill('0') 
                << partition.second 
                << ") INTO OUTFILE '" 
                << folderName << "/partition_data.csv' "
                << "FIELDS TERMINATED BY ',' "
                << "ENCLOSED BY '\"' "
                << "LINES TERMINATED BY '\\n'";

    // Export data
    if (mysql_query(conn, exportQuery.str().c_str())) {
        std::cerr << "Error exporting partition: " << mysql_error(conn) << std::endl;
        exit(1);
    }

    // Drop the partition
    std::stringstream dropQuery;
    dropQuery << "ALTER TABLE data DROP PARTITION p" 
              << partition.first 
              << std::setw(2) << std::setfill('0') 
              << partition.second;

    if (mysql_query(conn, dropQuery.str().c_str())) {
        std::cerr << "Error dropping partition: " << mysql_error(conn) << std::endl;
        exit(1);
    }

    std::cout << "Exported and dropped partition for " 
              << partition.second << "/" << partition.first << std::endl;
}

void deletePartitions(MYSQL *conn, int partitionsToDelete) {
    // Delete the specified number of partitions
    for (int i = 0; i < partitionsToDelete; ++i) {
        // Get oldest partition
        auto oldest = getOldestPartition(conn);
        
        // Dump and drop partition
        dumpPartitionToCSV(conn, oldest);
    }

    std::cout << "Completed deletion of " << partitionsToDelete << " partitions." << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: sudo ./main <partitions_to_delete>" << std::endl;
        exit(1);
    }

    int partitionsToDelete = std::stoi(argv[1]);
    if (partitionsToDelete <= 0) {
        std::cerr << "Number of partitions to delete must be greater than 0." << std::endl;
        exit(1);
    }

    MYSQL *conn;
    conn = mysql_init(nullptr);

    // Connect to MariaDB
    if (!mysql_real_connect(conn, "localhost", "my_user", "my_password", "my_database", 0, nullptr, 0)) {
        std::cerr << "Error connecting to database: " << mysql_error(conn) << std::endl;
        exit(1);
    }

    // Delete the specified number of partitions
    deletePartitions(conn, partitionsToDelete);

    mysql_close(conn);

    return 0;
}