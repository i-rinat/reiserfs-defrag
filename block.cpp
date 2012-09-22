#include "reiserfs.hpp"
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <assert.h>

const struct Block::key Block::zero_key = {0u, 0u, 0u, 0u};

Block::Block()
{
    memset(this->buf, 0, BLOCKSIZE);
    this->type = BLOCKTYPE_UNKNOWN;
    this->dirty = false;
    this->ref_count = 1;
}

Block::~Block()
{
    assert (not dirty);
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
            if (32 <= c && c < 127) printf("%c", c); else printf(".");
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
    std::cout << "-- dumpInternalNodeBlock()  ------------" << std::endl;
    std::cout << "level = " << this->level() << std::endl;
    std::cout << "key count = " << this->keyCount() << std::endl;
    std::cout << "free space = " << this->freeSpace() << std::endl;
    for (uint32_t k = 0; k < this->keyCount(); k ++) {
        const struct tree_ptr ptr = this->ptr(k);
        std::cout << "<" << ptr.block << ", " << ptr.size << "> ";
        this->key(k).dump_v1(std::cout, true);
    }
    const struct tree_ptr ptr = this->ptr(this->keyCount());
    std::cout << "<" << ptr.block << ", " << ptr.size << ">" << std::endl;

    std::cout << "========================================" << std::endl;
}

void
Block::dumpLeafNodeBlock() const
{
    std::cout << "Block::dumpLeafNodeBlock() stub" << std::endl;
}
