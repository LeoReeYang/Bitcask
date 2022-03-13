#include <map>
#include <mutex>
#include "header/logger.hpp"
#include "header/basicoperation.h"
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

std::mutex mutex_RW;

class Bitcask : BasicOperation
{
private:
    static uint64_t tstamp_;
    static uint64_t file_count;
    map<string, ValueIndex> index_;

public:
    Log logger;

    void index_add(const string &key, const ValueIndex &index)
    {
        index_[std::move(key)] = index;
    }

    void index_erase(const string &key)
    {
        index_.erase(std::move(key));
    }

    static uint64_t get_tstamp()
    {
        return tstamp_++;
    }

    void update_index(const string &key, size_t start_pos, size_t len)
    {
        ValueIndex temp(logger.write_prefix, start_pos, len);
        index_[key] = temp;
    }

public:
    Status get(const string &key, string *str_get);
    Status set(const string &key, const string &value);
    Status remove(const string &key);
    void list_keys();
    void recovery();
    void print_kv();
    Bitcask() {}
    ~Bitcask() {}
};

uint64_t Bitcask::tstamp_ = 0;
uint64_t Bitcask::file_count = 0;

Status Bitcask::set(const string &key, const string &value)
{
    size_t key_size = key.size(), value_size = value.size();
    size_t record_size = kInfoHeadSize + key_size + value_size + kValueTypeSize;

    Record record(get_tstamp(), key_size, value_size, key, value, kNewValue);

    std::lock_guard<std::mutex> lock(mutex_RW);

    size_t start_pos = logger.write(record, record_size);
    update_index(key, start_pos, record.value_size);

    return Status(OK, std::string(strerror(errno)));
}

Status Bitcask::get(const string &key, string *str_get)
{
    if (index_.find(key) != index_.end())
    {
        ValueIndex index = index_[key];
        char *value = new char[index.len];

        if (logger.read(index, value))
        {
            delete[] value;
            return Status(OK, std::string(strerror(errno)));
        }
        else
        {
            delete[] value;
            return Status(IoError, std::string(strerror(errno)));
        }
    }
    else
    {
        return Status(IoError, std::string(strerror(errno)));
    }
}

Status Bitcask::remove(const string &key)
{
    size_t key_size = key.size();
    size_t record_size = kInfoHeadSize + key_size + kValueTypeSize;

    Record record(get_tstamp(), key_size, 0, key, "", kRemoveValue);

    std::lock_guard<std::mutex> lock(mutex_RW);

    logger.write(record, record_size);
    index_erase(key);

    return Status(OK, std::string(strerror(errno)));
}

void Bitcask::recovery()
{
    char *head_buffer = new char[kInfoHeadSize];

    int file_nums = 0;
    std::string path = "/home/yu/Codes/1/Data";

    for (const auto &entry : fs::directory_iterator(path))
        file_nums++;

    for (size_t file = 0; file <= file_nums; file++)
    {
        int fd = open(path.c_str(), O_APPEND | O_CREAT | O_DIRECT | O_SYNC | O_RDWR);

        lseek(fd, 0, SEEK_CUR) != EOF;

        while (lseek(fd, 0, SEEK_CUR) != EOF)
        {
            uint64_t start_pos = lseek(fd, 0, SEEK_CUR);
            read(fd, head_buffer, kInfoHeadSize);

            InfoHeader *head = (InfoHeader *)head_buffer;

            if (head->value_size)
            {
                char *kv = new char[head->key_size + head->value_size];

                std::lock_guard<std::mutex> lock(mutex_RW);

                if (read(fd, kv, head->key_size + head->value_size))
                {
                    string key(kv, head->key_size);
                    size_t offset = lseek(fd, 0, SEEK_CUR);
                    ValueIndex index(file, offset, head->value_size);
                    index_add(key, index);
                    lseek(fd, kValueTypeSize, SEEK_CUR);
                }
                delete[] kv;
            }
            else
            {
                char *key = new char[head->key_size];

                std::lock_guard<std::mutex> lock(mutex_RW);

                if (read(fd, key, head->key_size) == head->key_size)
                {
                    index_erase(std::string(key, head->key_size));
                    lseek(fd, kValueTypeSize, SEEK_CUR);
                }

                delete[] key;
            }
        }
    }
    delete[] head_buffer;
}

void Bitcask::list_keys()
{
    cout << "keys lists:" << endl;
    for (const auto &kv : index_)
        cout << "key: " << kv.first << endl;
}

void Bitcask::print_kv()
{
    for (const auto &kv : index_)
    {
        string str_get;
        std::cout << "key: " << kv.first;
        get(kv.first, &str_get);
        std::cout << "value: " << str_get << std::endl;
    }
}
