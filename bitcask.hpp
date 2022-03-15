#include <map>
#include "header/logger.hpp"
#include "header/basicoperation.h"
#include <vector>
#include <cstring>

using namespace std;

class Bitcask : BasicOperation
{
private:
    static uint64_t tstamp_;
    map<string, ValueIndex> index_;
    size_t file_count = 0;
    Log *logger;
    map<string, Log *> logs;
    mutable std::shared_mutex logger_mutex;

    void recovery();
    void if_logs_insert(Log **);

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
        ValueIndex index(logger->get_fn(), value_offset, len);
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

    //同一个文件拿到logger锁的人才能操作logger去写入
    std::unique_lock lock(logger_mutex);
    {
        size_t value_offset = logger->write(record, record_size);
        update_index(key, value_offset, record.value_size);
        if_logs_insert(&logger);
    }

    return Status(OK, std::string(strerror(errno)));
}

Status Bitcask::get(const string &key, string *value)
{
    if (index_.find(key) != index_.end())
    {
        ValueIndex index;
        std::shared_lock lock(logger_mutex);
        {
            index = index_[key];
        }

        char *temp_value = new char[index.len];

        auto entry = logs.find(index.filename);

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

    std::unique_lock lock(logger_mutex);
    {
        logger->write(record, record_size);
        index_erase(key);
        if_logs_insert(&logger);
    }

    return Status(OK, std::string(strerror(errno)));
}

void Bitcask::recovery()
{
    size_t file_nums = get_file_nums();

    file_count = file_nums;

    char *head_buffer = new char[kInfoHeadSize];

    for (const auto &entry : fs::directory_iterator(DataPath))
    {
        ssize_t fd;
        std::string filename = string(entry.path().c_str());

        filename = filename.substr(filename.size() - 5); // get the file prefix to open

        logger = new Log(filename); // new a logger to handle the file

        if (logger->get_fn() == filename)
        {
            fd = logger->get_fd();
            if (fd != -1)
            {
                string file_id = filename.substr(0, filename.size() - 3);
                logs.insert(std::pair<string, Log *>(filename, logger));
            }
            else
                std::cout << "open file failed when get file descriptor." << std::endl;
        }
        else
            std::cout << "recovery logger open failed when open file :" << std::endl;

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
                std::cout << "logger id: " << filename
                          << " read for key failed." << strerror(errno) << std::endl;
            }

            if (head->value_size)
            {
                std::string key_value(key, head->key_size);
                size_t value_offset = lseek(fd, 0, SEEK_CUR);

                ValueIndex index(filename, value_offset, head->value_size);
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
        if_logs_insert(&logger);
    else
    {
        logger = new Log("0.log");
        logs.insert(std::pair<string, Log *>("0.log", logger));
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

void Bitcask::if_logs_insert(Log **logger) //   (Log *)*logger
{
    if ((*logger)->size() > kLogSize)
    {
        size_t new_id = file_count;

        std::string new_file(std::to_string(new_id) + std::string(".log"));

        *logger = new Log(new_file.c_str());
        logs.insert(std::pair<string, Log *>(new_file, *logger));

        std::cout << "new logger for file: " << new_id << std::endl;
    }
}
