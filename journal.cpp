#include "reiserfs.hpp"
#include <string.h>
#include <iostream>

Block::Block()
{
    memset(this->buf, 0, BLOCKSIZE);
}

FsJournal::FsJournal(int fd_)
{
    this->fd = fd_;
    std::cout << "FsJournal initialized" << std::cout;
}

