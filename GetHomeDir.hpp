#ifndef ASU_GETHOMEDIR
#define ASU_GETHOMEDIR

#include<string.h>
#include<unistd.h>
#include<sys/types.h>
#include<pwd.h>

inline std::string GetHomeDir(){
    return std::string(getpwuid(getuid())->pw_dir);
}

#endif
