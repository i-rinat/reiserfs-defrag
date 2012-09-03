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
    // std::cout << "FsJournal::beginTransaction stub" << std::endl;
}

void
FsJournal::commitTransaction()
{
    // std::cout << "FsJournal::commitTransaction stub" << std::endl;
    ::fsync(this->fd);
}

Block*
FsJournal::readBlock(uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_idx) * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return NULL;
    }
    Block *block_obj = new Block(this);
    ssize_t bytes_read = ::read (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock(" << block_idx << ")" << std::endl;
        return NULL;
    }
    block_obj->block = block_idx;
    return block_obj;
}

void
FsJournal::readBlock(Block &block_obj, uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, (off_t)block_idx * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return;
    }
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
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return;
    }
    ssize_t bytes_written = ::write (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_written) {
        std::cerr << "error: writeBlock(" << &block_obj << ")" << std::endl;
        return;
    }
}

void
FsJournal::writeBlockAt(Block *block_obj, uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_idx) * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return;
    }
    ssize_t bytes_written = ::write (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_written) {
        std::cerr << "error: writeBlockAt(" << block_obj << ", " << block_idx << ")" << std::endl;
        return;
    }
}

void
FsJournal::moveRawBlock(uint32_t from, uint32_t to)
{
    Block *block_obj = this->readBlock(from);
    this->writeBlockAt(block_obj, to);
}

void
FsJournal::releaseBlock(Block *block)
{
    delete block;
}
