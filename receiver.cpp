#include "receiver.h"
#include <iostream>

Receiver::Receiver(std::shared_ptr<DataStorage> storage) 
    : context(1)
    , zmq_subscriber(context, zmq::socket_type::sub)
    , m_storage(storage)
    , messageCount(0) {
    
    zmq_subscriber.connect("tcp://127.0.0.1:5555");
    zmq_subscriber.set(zmq::sockopt::subscribe, "");  // Subscribe to all messages
}

void Receiver::receiveData() {
    const int INSERT_THRESHOLD = 8;
    
    while (true) {
        zmq::message_t message;
        if (zmq_subscriber.recv(message, zmq::recv_flags::none)) {
            messageCount++;

            // Deserialize the received data
            msgpack::object_handle oh = msgpack::unpack(static_cast<const char*>(message.data()), message.size());
            msgpack::object deserialized = oh.get();

            // Unpack the data into a map
            std::unordered_map<std::string, msgpack::object> dataMap;
            deserialized.convert(dataMap);
            

            try {
                uint16_t commandID = dataMap.at("commandID").as<uint16_t>();

                // Handle based on the commandID using a switch statement
                switch (commandID) {
                    case PARSE_VERSION: m_storage->handleVersion(dataMap); break;
                    case PARSE_POWER: m_storage->handlePower(dataMap); break;
                    case PARSE_LASERHEAD_FLOW: m_storage->handleLaserheadFlow(dataMap); break;
                    case PARSE_PWM_MODULATION: m_storage->handlePWMModulation(dataMap); break;
                    case PARSE_DC_INFO: m_storage->handleDcInfo(dataMap); break;
                    case PARSE_RF_INFO: m_storage->handleRfInfo(dataMap); break;
                    case PARSE_SYSTEM_INFO: m_storage->handleSystemInfo(dataMap); break;
                    default: 
                        std::cerr << "Unknown commandID received: 0x" << std::hex << commandID << std::dec << std::endl;
                        break;
                }

                // Check if we've reached the insert threshold
                if (messageCount % INSERT_THRESHOLD == 0) {
                    m_storage->insertAllData();
                }

            } catch (const std::out_of_range&) {
                std::cerr << "Missing commandID key in received data" << std::endl;
            } catch (const msgpack::type_error&) {
                std::cerr << "Invalid type for commandID key in received data" << std::endl;
            }
        }
    }
}