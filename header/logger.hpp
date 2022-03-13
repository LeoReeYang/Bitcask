#include "record.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

std::string suffix = ".log";
std::string data_prefix = "data/";

const char *filename(size_t log_id)
{
    std::string fn = data_prefix + std::to_string(log_id) + suffix;
    return fn.c_str();
}

class Log
{
private:
    int id, fd;

    bool is_full() { return log_size > kLogSize ? true : false; }
    void switch_to_new_log();
    void new_log(char *, size_t);

public:
    Log()
    {
        fd = open(filename(write_prefix), O_APPEND | O_CREAT | O_DIRECT | O_SYNC | O_RDWR);
    }
    ~Log() {}

    size_t write_prefix = 0, read_prefix = 0;
    size_t log_size = 0;

    size_t write(Record &record, size_t record_size);
    bool read(ValueIndex &target, char *str);
};

size_t Log::write(Record &record, size_t record_size)
{
    char *temp = new char[record_size];

    record.build_buffer(temp);

    size_t offset = lseek(fd, 0, SEEK_CUR);

    if (::write(fd, temp, record_size) == record_size)
    {
        log_size += record_size;
        if (this->is_full())
        {
            close(fd);
            switch_to_new_log();
        }
    }
    delete[] temp;

    return offset + record_size - kValueTypeSize - record.value_size;
}

bool Log::read(ValueIndex &target, char *str)
{
    id = open(filename(target.file_id), O_APPEND | O_CREAT | O_DIRECT | O_SYNC | O_RDWR);
    if (lseek(id, target.offset, SEEK_SET) == target.offset)
    {
        ::pread(id, str, target.len, target.offset);
    }
}

void Log::switch_to_new_log()
{
    write_prefix++;
    log_size = 0;
    fd = open(filename(write_prefix), O_APPEND | O_CREAT | O_DIRECT | O_SYNC | O_RDWR);
}