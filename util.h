#pragma once

#include <string>

int get_log_id(const std::string &fname)
{
    std::string id_str = fname.substr(0, fname.size() - 4);
    return stoi(id_str);
}