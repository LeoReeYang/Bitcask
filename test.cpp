#include <iostream>
#include <unistd.h>

// using namespace std;

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <filesystem>
namespace fs = std::filesystem;

int main(int argc, char **argv)
{
    // int aflag = 0;
    // int bflag = 0;
    // char *cvalue = NULL;
    // int index;
    // int c;

    // opterr = 0;

    // while ((c = getopt(argc, argv, "abc:")) != -1)
    //     switch (c)
    //     {
    //     case 'a':
    //         aflag = 1;
    //         break;
    //     case 'b':
    //         bflag = 1;
    //         break;
    //     case 'c':
    //         cvalue = optarg;
    //         break;
    //     case '?':              // other characters
    //         if (optopt == 'c') // target character without argument needed
    //             fprintf(stderr, "Option -%c requires an argument.\n", optopt);
    //         else if (isprint(optopt)) // other characters
    //             fprintf(stderr, "Unknown option `-%c'.\n", optopt);
    //         else
    //             fprintf(stderr,
    //                     "Unknown option character `\\x%x'.\n",
    //                     optopt);
    //         return 1;
    //     default:
    //         abort();
    //     }

    // printf("aflag = %d, bflag = %d, cvalue = %s\n",
    //        aflag, bflag, cvalue);

    // for (index = optind; index < argc; index++)
    //     printf("Non-option argument %s\n", argv[index]);

    std::string path = "/home/yu/Codes/1";

    for (const auto &entry : fs::directory_iterator(path))
    {
        std::cout << entry.path() << std ::endl;
    }

    return 0;
}