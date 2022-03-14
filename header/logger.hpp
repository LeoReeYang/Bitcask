#include "record.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <filesystem>

std::string DataPath = "/home/yu/Codes/1/Data";

namespace fs = std::filesystem;

size_t getFileNums()
{
    int file_nums = 0;

    for (const auto &entry : fs::directory_iterator(DataPath))
        file_nums++;

    return file_nums;
}

std::string suffix = ".log";
std::string data_prefix = "Data/";

std::string filename(size_t log_id)
{
    std::string fn(data_prefix + std::to_string(log_id) + suffix);
    return fn;
}

class Log
{
private:
    int id, fd;
    size_t log_size = 0;

public:
    Log(size_t id)
    {
        fd = open(filename(id).c_str(), O_APPEND | O_SYNC | O_CREAT | O_RDWR);
        if (fd < 0)
        {
            std::cout << strerror(errno) << std::endl;
        }
    }
    ~Log() { close(fd); }

    size_t write(Record &record, size_t record_size);
    bool read(ValueIndex &target, char *str);
    size_t size() { return log_size; }
    size_t get_id() { return id; }
};

size_t Log::write(Record &record, size_t record_size)
{
    char *temp = new char[record_size];
    record.build_buffer(temp);

    if (fd < 0)
    {
        printf("%s", strerror(errno));
    }

    ssize_t offset = lseek(fd, 0, SEEK_CUR);
    if (lseek(fd, 0, SEEK_CUR) < 0)
    {
        std::cout << strerror(errno) << std::endl;
    }

    ssize_t write_nums = ::write(fd, (void *)temp, record_size);
    if (write_nums == record_size)
    {
        fsync(fd);
        log_size += record_size;
    }
    else
    {
        printf("%s\n", strerror(errno));
    }

    delete[] temp;

    return offset + record_size - kValueTypeSize - record.value_size;
}

bool Log::read(ValueIndex &target, char *str)
{
    id = open(filename(target.file_id).c_str(), O_APPEND | O_CREAT | O_DIRECT | O_SYNC | O_RDWR);

    if (lseek(id, target.offset, SEEK_SET) == target.offset)
    {
        ::pread(id, str, target.len, target.offset);
        return true;
    }
    return false;
}
