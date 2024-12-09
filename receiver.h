#include <zmq.hpp>
#include <msgpack.hpp>
#include <memory>
#include "dataStorage.h"

constexpr uint16_t PARSE_VERSION = 0x72;
constexpr uint16_t PARSE_POWER = 0x6B;
constexpr uint16_t PARSE_LASERHEAD_FLOW = 0x6a;
constexpr uint16_t PARSE_DC_INFO = 0xD1;
constexpr uint16_t PARSE_PWM_MODULATION = 0xE8;
constexpr uint16_t PARSE_RF_INFO = 0xEF;
constexpr uint16_t PARSE_SYSTEM_INFO = 0xa000;

class Receiver {
public:
    Receiver(std::shared_ptr<DataStorage> storage);
    void receiveData();

private:
    zmq::context_t context;
    zmq::socket_t zmq_subscriber;
    std::shared_ptr<DataStorage> m_storage;
    int messageCount;
    static constexpr int PUBLISH_INTERVAL = 15;
};