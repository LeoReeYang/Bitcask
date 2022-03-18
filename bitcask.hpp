#include <map>
#include "header/logger.hpp"
#include "header/basicoperation.h"
#include <vector>
#include <cstring>
#include <stdio.h>

using namespace std;

class Bitcask : BasicOperation
{
private:
    static uint64_t tstamp_;
    map<std::string, ValueIndex> index_;
    size_t file_count = 0;
    Log *logger;
    map<std::string, Log *> logs;
    mutable std::shared_mutex logger_mutex;
    std::mutex compact_mutex;

    size_t uncompacted = 0;

    void recovery();
    void if_switch_logger();
    void internel_compact(map<std::string, Log *> logs, map<std::string, ValueIndex> index, std::string cur_log_id);
    void compact()
    {
        std::thread compact_thread(&Bitcask::internel_compact, this, logs, index_, logger->get_fn());
        compact_thread.detach();
    }

public:
    void index_add(const std::string &key, const ValueIndex &index);

    void index_erase(const std::string &key);

    void update_index(const string &key, size_t value_offset, size_t len);

    static uint64_t get_tstamp()
    {
        return tstamp_++;
    }

public:
    Status get(const std::string &key, std::string *str_get);
    Status set(const std::string &key, const std::string &value);
    Status remove(const std::string &key);

    void list_keys();
    void print_kv();
    Bitcask() { recovery(); }
    ~Bitcask() {}
};
uint64_t Bitcask::tstamp_ = 0;

void Bitcask::index_add(const std::string &key, const ValueIndex &index)
{
    index_[std::move(key)] = index;
}

void Bitcask::index_erase(const std::string &key)
{
    index_.erase(std::move(key));
}

void Bitcask::update_index(const string &key, size_t value_offset, size_t len)
{
    ValueIndex index(logger->get_fn(), value_offset, len);
    index_[std::move(key)] = index;
}

Status Bitcask::set(const std::string &key, const std::string &value)
{
    size_t key_size = key.size(), value_size = value.size();
    size_t record_size = kInfoHeadSize + key_size + value_size + kValueTypeSize;

    Record record(get_tstamp(), key_size, value_size, key, value, kNewValue);

    if (index_.find(key) != index_.end())
    {
        uncompacted += index_[key].len + kInfoHeadSize + kValueTypeSize + key_size;
        std::cout << "uncompacted : " << uncompacted << std::endl;
        if (uncompacted >= kCompactThreshold)
        {
            std::cout << "call compact()" << std::endl;
            std::lock_guard lock(compact_mutex);
            {
                compact();
                uncompacted = 0;
            }
        }
    }

    //同一个文件拿到logger锁的人才能操作logger去写入
    std::unique_lock lock(logger_mutex);
    {
        size_t value_offset = logger->write(record, record_size);
        update_index(key, value_offset, record.value_size);
        if_switch_logger();
    }

    return Status(OK, std::string(strerror(errno)));
}

Status Bitcask::get(const std::string &key, std::string *value)
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
            *value = std::string(temp_value, index.len);
            delete[] temp_value;
            return Status(OK, std::string(strerror(errno)));
        }
        else
        {
            delete[] temp_value;
            return Status(IoError, std::string(std::string("read value failed .") + strerror(errno)));
        }
    }
    else
    {
        return Status(IoError, std::string(strerror(errno)));
    }
}

Status Bitcask::remove(const std::string &key)
{
    size_t key_size = key.size();
    size_t record_size = kInfoHeadSize + key_size + kValueTypeSize;

    Record record(get_tstamp(), key_size, 0, key, "", kRemoveValue);

    if (index_.find(key) != index_.end())
    {
        uncompacted += kInfoHeadSize + kValueTypeSize + key_size;
        if (uncompacted >= kCompactThreshold)
        {
            std::cout << "uncompacted : " << uncompacted << std::endl;
            std::lock_guard lock(compact_mutex);
            {
                compact();
                uncompacted = 0;
            }
        }
    }

    std::unique_lock lock(logger_mutex);
    {
        logger->write(record, record_size);
        index_erase(key);
        if_switch_logger();
    }

    return Status(OK, std::string(strerror(errno)));
}

void Bitcask::recovery()
{
    file_count = get_file_nums();
    char *head_buffer = new char[kInfoHeadSize];

    for (const auto &entry : fs::directory_iterator(DataPath))
    {
        ssize_t fd;
        std::string filename = std::string(entry.path().c_str());

        filename = filename.substr(DataPath.size(), filename.size() - DataPath.size()); // get the file prefix to open

        logger = new Log(filename); // new a logger to handle the file

        if (logger->get_fn() == filename) //"xxx.log"
        {
            fd = logger->get_fd();
            if (fd != -1)
                logs.insert(std::pair<std::string, Log *>(filename, logger));
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
                std::cout << "read head_buffer failed" << strerror(errno) << std::endl;

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

                if (index_.find(key_value) != index_.end()) // if key exit..
                {
                    uncompacted += index_[key].len + kInfoHeadSize + kValueTypeSize + head->key_size + head->value_size;
                }

                index_add(key_value, index);                            // update key with the index in the Memory
                lseek(fd, head->value_size + kValueTypeSize, SEEK_CUR); // ignore the value and valueType section
            }
            else
            {
                uncompacted += kInfoHeadSize + head->key_size + kValueTypeSize;
                index_erase(std::string(key, head->key_size));
                lseek(fd, kValueTypeSize, SEEK_CUR);
            }
            delete[] key;
        }
    }

    std::cout << "recovery umcompacted: " << uncompacted << std::endl;

    if (uncompacted >= kCompactThreshold)
    {
        // compact();
        std::thread compact_thread(&Bitcask::internel_compact, this, logs, index_, logger->get_fn());
        // compact_thread.detach();
        compact_thread.join();
    }

    if (file_count != 0)
        if_switch_logger();
    else
    {
        logger = new Log("0.log");
        logs.insert(std::pair<std::string, Log *>("0.log", logger));
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

void Bitcask::if_switch_logger() //   (Log *)*logger
{
    if (logger->size() > kLogSize)
    {
        size_t new_id = file_count + 1;
        file_count++;

        std::string new_file(std::to_string(new_id) + std::string(".log"));

        logger = new Log(new_file);
        logs.insert(std::pair<string, Log *>(new_file, logger));

        std::cout << "new logger for file: " << new_id << std::endl;
    }
}

void Bitcask::internel_compact(map<std::string, Log *> logs, map<std::string, ValueIndex> index, std::string cur_log_name)
{
    std::cout << "compact:======================================>" << std::endl;

    map<std::string, Log *> new_logs;
    map<std::string, ValueIndex> new_index;

    std::string working_prefix = cur_log_name.substr(0, cur_log_name.size() - 4);
    size_t working_log_id = stoi(working_prefix); // 拿到正在写的log的数组前缀

    size_t new_log_start = file_count + 100;                       // new log file prefix
    std::string new_file = std::to_string(new_log_start) + suffix; // make the "xxx.log" string
    Log *target = new Log(new_file);
    new_logs.insert(std::pair<std::string, Log *>(new_file, target));

    for (auto elem : index) // 遍历索引表
    {                       // key->file,offset,len
        std::string log_id = elem.second.filename;
        size_t cur_log_id = stoi(log_id.substr(0, log_id.size() - 4)); //拿到遍历项目的前缀

        if (cur_log_id < working_log_id)
        {
            auto work_logger = logs[elem.second.filename];
            char *temp_value = new char[elem.second.len];

            work_logger->read(elem.second, temp_value);

            std::string test_value = std::string(temp_value, elem.second.len); // get value

            // build record
            Record temp_record(Bitcask::get_tstamp(), elem.first.size(), elem.second.len, elem.first, test_value, kNewValue);
            size_t record_size = kInfoHeadSize + elem.first.size() + elem.second.len + kValueTypeSize;
            size_t value_offset = target->write(temp_record, record_size);

            // write into new_index
            new_index[elem.first] = std::move(ValueIndex(target->get_fn(), value_offset, elem.second.len));

            if (target->size() > kLogSize) // when to switch to new log file
            {
                size_t new_id = new_log_start + 1;
                new_log_start++;
                std::string new_file(std::to_string(new_id) + std::string(".log"));

                target = new Log(new_file);
                new_logs.insert(std::pair<std::string, Log *>(new_file, target));

                std::cout << "new logger for file: " << new_id << std::endl;
            }
            delete[] temp_value;
        }
    }

    for (auto &log : logs) // delete all the old files
    {
        // std::cout << DataPath + log.first << std::endl;
        ::remove((DataPath + log.first).c_str());
    }

    Bitcask::logs.merge(new_logs); // 利用merge不会合并相同key的原理先合并不同的
    //
    for (auto &entry : new_logs)
    {
        Bitcask::logs[entry.first] = new Log(entry.first);
    }
    Bitcask::index_.merge(new_index); // key and ValueIndex
    for (auto &entry : new_index)
    {
        // ValueIndex a = new ValueIndex(entry.second.filename, entry.second.offset, entry.second.len);
        Bitcask::index_[entry.first] = entry.second;
    }
}