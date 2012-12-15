/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <iostream>
#include <string.h>
#include <stdio.h>

const Block::key_t Block::zero_key(KEY_V0, 0u, 0u, 0u, 0u);
const Block::key_t Block::largest_key(KEY_V0, ~0u, ~0u, ~0u, ~0u);

Block::Block()
{
    memset(this->buf, 0, BLOCKSIZE);
    this->type = BLOCKTYPE_UNKNOWN;
    this->dirty = false;
    this->ref_count = 1;
}

Block::~Block()
{
    assert1 (not dirty);
}

void
Block::rawDump() const
{
    for (unsigned int row = 0; row < BLOCKSIZE/16; row ++) {
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

void
Block::checkLeafNode() const
{
    // check level
    if (this->level() != TREE_LEVEL_LEAF) {
        std::cout << "leaf node #" << this->block << " has wrong level (" <<
            this->level() << ")" << std::endl;
        fatal("fs inconsistent");
    }

    // check keys ordering
    for (uint32_t k = 0; k < this->itemCount() - 1; k ++) {
        if (this->itemHeader(k).key >= this->itemHeader(k + 1).key) {
            std::cout << "leaf node #" << this->block << " has wrong key ordering" << std::endl;
            fatal("fs inconsistent");
        }
    }
    // item must entirely reside inside a block
    for (uint32_t k = 0; k < this->itemCount(); k ++) {
        const uint32_t end_pos = static_cast<uint32_t>(this->itemHeader(k).offset) +
                                                       this->itemHeader(k).length;
        if (end_pos > BLOCKSIZE) {
            std::cout << "leaf node #" << this->block << " has wrong items" << std::endl;
            fatal("fs inconsistent");
        }
    }

    // TODO: check that items do not overlap
}

void
Block::checkInternalNode() const
{
    // check level
    if (this->level() <= TREE_LEVEL_LEAF || this->level() > TREE_LEVEL_MAX) {
        std::cout << "internal node #" << this->block << " has wrong level (" <<
            this->level() << ")" << std::endl;
        fatal("fs inconsistent");
    }

    // check item_nr and free_space consistency. Each block has 24-byte header and consists of
    // 16-byte keys, 8-byte pointers and free space. Check if all this sum up to block size
    if (BLOCKSIZE != 24 + 16 * this->keyCount() + 8 * this->ptrCount() + this->freeSpace()) {
        std::cout << "internal node #" << this->block <<
            " has wrong item_nr and free_space combination" << std::endl;
        fatal("fs inconsistent");
    }

    // check key ordering
    for (uint32_t k = 0; k < this->keyCount() - 1; k ++) {
        if (this->key(k) >= this->key(k+1)) {
            std::cout << "intenal node #" << this->block << " has wrong key ordering" << std::endl;
            fatal("fs inconsistent");
        }
    }

    // TODO: check that all pointers refer blocks inside fs
}
