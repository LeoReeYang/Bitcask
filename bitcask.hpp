#include <map>
#include <mutex>
#include "header/logger.hpp"
#include "header/basicoperation.h"
#include <vector>

using namespace std;

std::mutex mutex_RW;

class Bitcask : BasicOperation
{
private:
    static uint64_t tstamp_;
    static uint64_t file_count;
    map<string, ValueIndex> index_;

public:
    Log *logger;

    map<size_t, Log> logs;

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
        ValueIndex temp(logger->get_id(), start_pos, len);
        index_[key] = temp;
    }

public:
    Status get(const string &key, string *str_get);
    Status set(const string &key, const string &value);
    Status remove(const string &key);
    void list_keys();
    void recovery(size_t file_nums);
    void print_kv();
    Bitcask()
    {
        size_t file_nums = getFileNums();
        recovery(file_nums);

        if (file_nums == 0)
        {
            size_t id = 0;
            logger = new Log(id);
            logs.insert(std::pair<size_t, Log>(id, *logger));
        }
        else
        {
            size_t id = file_nums + 1;
            logger = new Log(id);
            logs.insert(std::pair<size_t, Log>(id, *logger));
        }
    }
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

    size_t start_pos = logger->write(record, record_size);
    update_index(key, start_pos, record.value_size);

    if (logger->size() > kLogSize)
    {
        size_t new_id = logger->get_id() + 1;
        logger = new Log(new_id);
        logs.insert(std::pair<size_t, Log>(new_id, *logger));
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

        if (entry->second.read(index, temp_value))
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

    std::lock_guard<std::mutex> lock(mutex_RW);

    logger->write(record, record_size);
    index_erase(key);

    if (logger->size() > kLogSize)
    {
        size_t new_id = logger->get_id() + 1;
        logger = new Log(new_id);
        logs.insert(std::pair<size_t, Log>(new_id, *logger));
    }

    return Status(OK, std::string(strerror(errno)));
}

void Bitcask::recovery(size_t file_nums)
{
    char *head_buffer = new char[kInfoHeadSize];

    for (size_t file = 0; file < file_nums; file++)
    {
        int fd = open(filename(file).c_str(), O_RDONLY);

        while (lseek(fd, 0, SEEK_CUR) != EOF)
        {
            uint64_t start_pos = lseek(fd, 0, SEEK_SET);
            read(fd, (void *)head_buffer, kInfoHeadSize);

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
