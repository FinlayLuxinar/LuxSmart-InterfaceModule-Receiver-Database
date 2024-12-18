#include <zmq.hpp>
#include <msgpack.hpp>
#include <iostream>
#include <string>
#include <unordered_map>
#include <memory>
#include <thread>
#include <chrono>
#include <ctime>
#include <atomic>

#include "receiver.h"
#include "dataStorage.h"
#include "storageManager.h"

// Function to wait until the start of the next hour
void waitUntilNextHour() {
    auto now = std::chrono::system_clock::now();
    auto nextHour = std::chrono::time_point_cast<std::chrono::minutes>(now) + std::chrono::minutes(1);
    std::this_thread::sleep_until(nextHour);
}

// Thread function to check disk usage
void monitorDiskUsage(std::shared_ptr<StorageManager> storageManager, std::atomic<bool>& running, const std::string& outputFolder) {
    while (running) {
        // Wait until the next hour
        waitUntilNextHour();

        // Check disk usage
        if (storageManager->checkDiskUsage()) {
            std::cout << "Disk usage is full!" << std::endl;
            // Trigger storage reduction by calling reduceStorage
            storageManager->reduceStorage(outputFolder);  // For example, delete data older than 30 days
        }
        else {
            std::cout << "Disk usage is below threshold." << std::endl;
        }
    }
}


int main() {
    // Create shared pointer for StorageManager
    auto storageManager = std::make_shared<StorageManager>("localhost", "my_user", "my_password", "my_database");
    auto storage = std::make_shared<DataStorage>();

    // Atomic flag to manage the lifetime of the thread
    std::atomic<bool> running(true);

    // Specify the folder where CSV files should be exported
    std::string outputFolder = "/home/raspberry/database";

    // Start the disk usage monitor thread
    std::thread diskMonitorThread(monitorDiskUsage, storageManager, std::ref(running), outputFolder);

    // Create receiver with shared resources
    Receiver receiver(storage);
    receiver.receiveData();

    // Join the thread (optional if you need clean termination, or handle stopping it)
    running = false;
    if (diskMonitorThread.joinable()) {
        diskMonitorThread.join();
    }

    return 0;
}

