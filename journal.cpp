#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <stdio.h>

Block::Block()
{
    memset(this->buf, 0, BLOCKSIZE);
    this->type = BLOCKTYPE_UNKNOWN;
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

void
Block::formattedDump() const
{
    switch (this->type) {
        case BLOCKTYPE_UNKNOWN:
            std::cout << "unknown block" << std::endl;
            break;
        case BLOCKTYPE_INTERNAL:
            this->dumpInternalNodeBlock();
            break;
        case BLOCKTYPE_LEAF:
            this->dumpLeafNodeBlock();
            break;
        case BLOCKTYPE_UNFORMATTED:
            this->rawDump();
            break;
        default:
            std::cout << "block type error" << std::endl;
    }
}

void
Block::dumpInternalNodeBlock() const
{
    std::cout << "Block::dumpInternalNodeBlock() stub" << std::endl;
    struct blockheader *bh = (struct blockheader *)buf;
    std::cout << "-- dumpInternalNodeBlock()  ------------" << std::endl;
    std::cout << "level = " << bh->bh_level << std::endl;
    std::cout << "Nr. items = " << bh->bh_nr_items << std::endl;
    std::cout << "free space = " << bh->bh_free_space << std::endl;
    for (int k = 0; k < bh->bh_nr_items; k ++) {
        const uint32_t &dirid = reinterpret_cast<const uint32_t&>(buf[24 + 16*k]);
        const uint16_t &objid = reinterpret_cast<const uint32_t&>(buf[24 + 16*k + 4]);
        const uint32_t &part1 = reinterpret_cast<const uint32_t&>(buf[24 + 16*k + 8]);
        const uint32_t &part2 = reinterpret_cast<const uint32_t&>(buf[24 + 16*k + 12]);
        const uint64_t offset = ((uint64_t)(part2 & 0x0FFFFFFF) << 32) + part1;
        const uint32_t type = (part2 & 0xF0000000) >> 28;

        std::cout << dirid << ", " << objid << ", " << offset << ", " << type << std::endl;
    }
    const int ofs = 24 + 16 * bh->bh_nr_items;
    for (int k = 0; k < bh->bh_nr_items + 1; k ++) {
        const uint32_t &blocknumber = reinterpret_cast<const uint32_t&>(buf[ofs + 8*k]);
        const uint16_t &size = reinterpret_cast<const uint16_t&>(buf[ofs + 8*k + 4]);
        std::cout << blocknumber << ", " << size << std::endl;
    }

    std::cout << "========================================" << std::endl;
}

void
Block::dumpLeafNodeBlock() const
{
    std::cout << "Block::dumpLeafNodeBlock() stub" << std::endl;
}

void
Block::setType(int type_)
{
    this->type = type_;
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
