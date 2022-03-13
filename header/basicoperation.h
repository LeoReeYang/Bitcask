#include <string>
#include "error.h"
class BasicOperation
{
public:
    virtual Status get(const std::string &key, std::string *str_get) = 0;
    virtual Status set(const std::string &key, const std::string &value) = 0;
    virtual Status remove(const std::string &key) = 0;
};
