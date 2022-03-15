#include <map>
#include "header/logger.hpp"
#include "header/basicoperation.h"
#include <vector>
#include <cstring>

std::mutex logger_mutex;

using namespace std;

class Bitcask : BasicOperation
{
private:
    static uint64_t tstamp_;
    map<string, ValueIndex> index_;
    Log *logger;
    map<size_t, Log *> logs;

    void recovery();
    void if_logs_insert(Log *);

public:
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

    void update_index(const string &key, size_t value_offset, size_t len)
    {
        ValueIndex index(logger->get_id(), value_offset, len);
        index_[std::move(key)] = index;
    }

public:
    Status get(const string &key, string *str_get);
    Status set(const string &key, const string &value);
    Status remove(const string &key);

    void list_keys();
    void print_kv();
    Bitcask()
    {
        recovery();
    }
    ~Bitcask() {}
};

uint64_t Bitcask::tstamp_ = 0;

Status Bitcask::set(const string &key, const string &value)
{
    size_t key_size = key.size(), value_size = value.size();
    size_t record_size = kInfoHeadSize + key_size + value_size + kValueTypeSize;

    Record record(get_tstamp(), key_size, value_size, key, value, kNewValue);

    std::lock_guard<std::mutex> lock(logger_mutex); //同一个文件拿到logger锁的人才能操作logger去写入
    {
        size_t value_offset = logger->write(record, record_size);
        update_index(key, value_offset, record.value_size);
        if_logs_insert(logger);
    }

    return Status(OK, std::string(strerror(errno)));
}

Status Bitcask::get(const string &key, string *value)
{
    if (index_.find(key) != index_.end())
    {
        ValueIndex index = index_[key];
        char *temp_value = new char[index.len];

        auto entry = logs.find(index.file_id);

        if ((entry->second)->read(index, temp_value))
        {
            *value = string(temp_value, index.len);
            delete[] temp_value;
            return Status(OK, std::string(strerror(errno)));
        }
        else
        {
            delete[] temp_value;
            return Status(IoError, std::string(string("read value failed .") + strerror(errno)));
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

    logger->write(record, record_size);
    index_erase(key);

    if_logs_insert(logger);

    return Status(OK, std::string(strerror(errno)));
}

void Bitcask::recovery()
{
    size_t file_nums = get_file_nums();

    char *head_buffer = new char[kInfoHeadSize];

    for (size_t file = 0; file < file_nums; file++)
    {
        ssize_t fd;
        logger = new Log(file);

        if (logger->get_id() == file)
        {
            fd = logger->get_fd();
            if (fd != -1)
                logs.insert(std::pair<size_t, Log *>(file, logger));
            else
                std::cout << "open file failed when get file descriptor." << std::endl;
        }
        else
            std::cout << "recovery logger open failed when open file :" << file << std::endl;

        size_t file_size = lseek(fd, 0, SEEK_END);
        size_t offset = lseek(fd, 0, SEEK_SET);

        while (lseek(fd, 0, SEEK_CUR) != file_size) // file_size + 1 ?
        {
            if (read(fd, (void *)head_buffer, kInfoHeadSize) != kInfoHeadSize)
            {
                std::cout << "read head_buffer failed" << strerror(errno) << std::endl;
            }

            InfoHeader *head = (InfoHeader *)head_buffer;
            char *key = new char[head->key_size];

            if (read(fd, (void *)key, head->key_size) != head->key_size)
            {
                std::cout << "logger id: " << file
                          << " read for key failed." << strerror(errno) << std::endl;
            }

            if (head->value_size)
            {
                std::string key_value(key, head->key_size);
                size_t value_offset = lseek(fd, 0, SEEK_CUR);

                ValueIndex index(file, value_offset, head->value_size);
                index_add(key_value, index); // update key with the index in the Memory

                lseek(fd, head->value_size + kValueTypeSize, SEEK_CUR); // ignore the value and valueType section
            }
            else
            {
                index_erase(std::string(key, head->key_size));
                lseek(fd, kValueTypeSize, SEEK_CUR);
            }
            delete[] key;
        }
    }

    if (file_nums != 0)
        if_logs_insert(logger);
    else
    {
        logger = new Log(0);
        logs.insert(std::pair<size_t, Log *>(0, logger));
    }

    delete[] head_buffer;
}

void Bitcask::list_keys()
{
    std::cout << "keys lists:" << endl;
    for (const auto &kv : index_)
        std::cout << "key: " << kv.first << endl;
}

void Bitcask::print_kv()
{
    for (const auto &kv : index_)
    {
        string str_get;
        std::cout << "key: " << kv.first << "    ";

        get(kv.first, &str_get);
        std::cout << "value: " << str_get << std::endl;
    }
}

void Bitcask::if_logs_insert(Log *logger)
{
    if (logger->size() > kLogSize)
    {
        size_t new_id = logger->get_id() + 1;

        logger = new Log(new_id);
        logs.insert(std::pair<size_t, Log *>(new_id, logger));

        std::cout << "new logger for file: " << new_id << std::endl;
    }
}