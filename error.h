#include <string>

enum ErrorCode
{
    OK,
    IoError,
    InvalidArgument,
    Corrupted,
};

struct Status
{
    ErrorCode code;
    std::string message;
    Status(ErrorCode code, std::string message)
        : code(code), message(message) {}

    // friend ostream &operator<<(ostream &output, const Status &obj)
    // {
    //     output << "Code : " << obj.code << std::endl
    //            << "Message : " << obj.message << std::endl;
    //     return output;
    // }
};
