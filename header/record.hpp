#include "crc32.h"
#include <cstring>
enum InfoType
{
    kNewValue = '0',
    kRemoveValue = '1'
};

const uint32_t kCRCSize = sizeof(uint32_t);
const uint32_t kTimeStampSize = sizeof(uint64_t);
const uint64_t kValueLenSize = sizeof(uint64_t);
const uint64_t kKeyLenSize = sizeof(uint64_t);
const uint64_t kRecordSize = sizeof(uint64_t);
const uint64_t kValueTypeSize = sizeof(InfoType);
const size_t kLogSize = 60;

struct ValueIndex
{
    uint64_t file_id;
    uint64_t offset, len;
    ValueIndex(uint64_t file_id, uint64_t offset, uint64_t len) : file_id(file_id), offset(offset), len(len) {}
    ValueIndex() {}
    ~ValueIndex() {}
};

struct InfoHeader
{
    uint32_t crc = 0;
    uint64_t time_stamp;
    uint64_t key_size;
    uint64_t value_size;

    virtual void build_buffer(char *temp) = 0;

    InfoHeader() {}
    InfoHeader(uint64_t tstamp, uint64_t key_size, uint64_t value_size)
        : time_stamp(tstamp), key_size(key_size), value_size(value_size)
    {
        crc = CRC::Calculate(&time_stamp, kTimeStampSize, CRC::CRC_32());
        crc = CRC::Calculate(&key_size, kKeyLenSize, CRC::CRC_32(), crc);
        crc = CRC::Calculate(&value_size, kValueLenSize, CRC::CRC_32(), crc);
    }
    ~InfoHeader() {}
};
const size_t kInfoHeadSize = sizeof(InfoHeader);

struct Record : public InfoHeader
{
    const std::string &key, &value;
    InfoType value_type;

    Record(uint64_t tstamp, uint64_t key_size, uint64_t value_size, const std::string &key, const std::string &value, InfoType value_type)
        : InfoHeader(tstamp, key_size, value_size), key(key), value(value), value_type(value_type)
    {
        crc = CRC::Calculate(key.c_str(), key_size, CRC::CRC_32(), crc);
        crc = CRC::Calculate(value.c_str(), value_size, CRC::CRC_32(), crc);
        crc = CRC::Calculate(&value_type, kValueTypeSize, CRC::CRC_32(), crc);
    }
    ~Record() {}

    void build_buffer(char *temp)
    {
        memcpy(temp, (void *)(InfoHeader *)this, kInfoHeadSize);
        memcpy(temp + kInfoHeadSize, (void *)key.c_str(), key_size);
        memcpy(temp + kInfoHeadSize + key_size, (void *)value.c_str(), value_size);
        memcpy(temp + kInfoHeadSize + key_size + value_size, (void *)&value_type, kValueTypeSize);
    }
};