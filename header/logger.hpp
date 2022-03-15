#include "record.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <mutex>
#include <iostream>
#include <thread>

namespace fs = std::filesystem;

std::mutex mutex_RW;

const std::string DataPath = "/home/yu/Codes/Bitcask/Data";
std::string suffix = ".log";
std::string prefix = "Data/";

size_t get_file_nums()
{
    int file_nums = 0;

    for (const auto &entry : fs::directory_iterator(DataPath))
        file_nums++;

    return file_nums;
}

std::string filename(size_t log_id)
{
    std::string fn(prefix + std::to_string(log_id) + suffix);
    return fn;
}

class Log
{
private:
    size_t id;
    ssize_t fd;
    size_t log_size = 0;

public:
    Log(size_t id) : id(id)
    {
        fd = open(filename(id).c_str(), O_APPEND | O_SYNC | O_CREAT | O_RDWR, S_IRWXU);
        if (fd < 0)
            std::cout << strerror(errno) << std::endl;
    }
    ~Log() { close(fd); }

    size_t write(Record &record, size_t record_size);
    bool read(ValueIndex &target, char *str);

    size_t size() { return log_size; }
    size_t get_id() { return id; }
    size_t get_fd() { return fd; }
};

size_t Log::write(Record &record, size_t record_size)
{
    char *temp = new char[record_size];
    record.build_buffer(temp);
    size_t write_nums;

    if (fd < 0)
    {
        std::cout << "logger open file failed at id : " << id << std::endl
                  << strerror(errno) << std::endl;
        exit(-1);
    }

    size_t offset = lseek(fd, 0, SEEK_CUR);
    if (offset < 0)
    {
        std::cout << "logger lseek failed at id : " << id << std::endl
                  << strerror(errno) << std::endl;
        exit(-1);
    }

    // std::lock_guard<std::mutex> lock(mutex_RW);
    {
        write_nums = ::write(fd, (void *)temp, record_size);
    }

    if (write_nums == record_size)
    {
        fsync(fd);
        log_size += record_size;

        std::thread::id this_id = std::this_thread::get_id();

        std::cout << "Thread id: " << this_id << "  Successfully write: "
                  << '"' << record.value << '"' << "  file id :" << id << std::endl;
    }
    else
    {
        std::cout << "logger write failed at id : " << id << std::endl
                  << strerror(errno) << std::endl;
        exit(-1);
    }

    delete[] temp;

    return offset + record_size - kValueTypeSize - record.value_size;
}

bool Log::read(ValueIndex &target, char *str)
{
    size_t lseek_offset = lseek(fd, target.offset, SEEK_SET);

    if (lseek_offset == target.offset)
    {
        ::pread(fd, str, target.len, target.offset);
        return true;
    }
    else
    {
        std::cout << "lseek error" << std::endl
                  << strerror(errno) << std::endl;
    }
    return false;
}
