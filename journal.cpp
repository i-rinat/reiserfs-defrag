#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <stdio.h>

Block::Block()
{
    memset(this->buf, 0, BLOCKSIZE);
}

void
Block::rawDump() const
{
    for (int row = 0; row < BLOCKSIZE/16; row ++) {
        printf ("%08X  ", row*16 + this->block*BLOCKSIZE);

        int k;
        for (k = 0; k < 8; k ++) printf("%02X ", (unsigned char)buf[row*16+k]);
        printf ("|");
        for (k = 8; k < 16; k ++) printf(" %02X",(unsigned char)buf[row*16+k]);
        printf ("  |");
        for (k = 0; k < 16; k ++) {
            char c = buf[row*16+k];
            if (32 <= c && c < 128) printf("%c", c); else printf(".");
        }
        printf ("|\n");
    }
}

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
FsJournal::readBlock(uint32_t block)
{
    off_t new_ofs = ::lseek (this->fd, (off_t)block * BLOCKSIZE, SEEK_SET);
    Block *block_obj = new Block();
    ssize_t bytes_read = ::read (this->fd, block_obj->ptr(), BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock("<<block << ")" << std::endl;
        return 0;
    }
    block_obj->block = block;
    return block_obj;
}

void
FsJournal::releaseBlock(Block *block)
{
    delete block;
}
