#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>


FsJournal::FsJournal(int fd_)
{
    this->fd = fd_;
    std::cout << "FsJournal initialized" << std::endl;
}

void
FsJournal::beginTransaction()
{
    std::cout << "FsJournal::beginTransaction stub" << std::endl;
}

void
FsJournal::commitTransaction()
{
    std::cout << "FsJournal::commitTransaction stub" << std::endl;
}

Block*
FsJournal::readBlock(uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_idx) * BLOCKSIZE, SEEK_SET);
    Block *block_obj = new Block(this);
    ssize_t bytes_read = ::read (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock(" << block_idx << ")" << std::endl;
        return 0;
    }
    block_obj->block = block_idx;
    return block_obj;
}

void
FsJournal::readBlock(Block &block_obj, uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, (off_t)block_idx * BLOCKSIZE, SEEK_SET);
    ssize_t bytes_read = ::read (this->fd, block_obj.buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock(" << &block_obj << ", " << block_idx << ")" << std::endl;
        return;
    }
    block_obj.block = block_idx;
}

void
FsJournal::writeBlock(Block *block_obj)
{
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_obj->block) * BLOCKSIZE, SEEK_SET);
    ssize_t bytes_written = ::write (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_written) {
        std::cerr << "error: writeBlock(" << block_obj->block << ")" << std::endl;
        return;
    }
}

void
FsJournal::releaseBlock(Block *block)
{
    delete block;
}
