#include <zmq.hpp>
#include <msgpack.hpp>
#include <iostream>
#include <string>
#include <unordered_map>
#include <memory>

#include "receiver.h"
#include "dataStorage.h"

int main() {
    // Create shared pointers for DataStorage
    auto storage = std::make_shared<DataStorage>();

    // Create receiver with shared resources
    Receiver receiver(storage);
    receiver.receiveData();

    return 0;
}