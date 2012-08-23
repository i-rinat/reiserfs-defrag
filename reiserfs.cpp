#include "reiserfs.hpp"
#include <iostream>
#include <unistd.h>
#include <errno.h>
#include <string.h>


ReiserFs::ReiserFs()
{

}

ReiserFs::~ReiserFs()
{

}

int
ReiserFs::open(std::string name)
{
    std::cout << "open " << name << std::endl;
    this->fname = name;
    fd = ::open(name.c_str(), O_RDWR | O_SYNC | O_LARGEFILE);
    if (-1 == fd) {
        std::cerr << "error: can't open file `" << name << "', errno = " << errno << std::endl;
        return RFSD_FAIL;
    }

    this->readSuperblock();
    return RFSD_OK;
}

void
ReiserFs::readSuperblock()
{
    std::cout << "readSuperblock" << std::endl;
    const off_t sb_offset = 65536;
    off_t res = ::lseek (this->fd, sb_offset, SEEK_SET);
    if (res != sb_offset) {
        std::cerr << "error: can't lseek, errno = " << errno << std::endl;
        return;
    }

    char buf[4096];
    ssize_t bytes_read = ::read (this->fd, buf, sizeof(buf));
    if (bytes_read == -1) {
        std::cerr << "error: can't read, errno = " << errno << ", " << strerror(errno) << std::endl;
        std::cout << sizeof(buf) << std::endl;
        std::cout << this->fd << std::endl;
        return;
    }

    memcpy(&this->sb, buf, sizeof(this->sb));
}

int
ReiserFs::close()
{
    std::cout << "close " << this->fname << std::endl;
}

int
ReiserFs::moveBlock(uint32_t from, uint32_t to)
{
    std::cout << "from: " << from << ", to: " << to << std::endl;
}

int
ReiserFs::beginTransaction()
{
    std::cout << "beginTransaction" << std::endl;
}

int
ReiserFs::commitTransaction()
{
    std::cout << "commitTransaction" << std::endl;
}
