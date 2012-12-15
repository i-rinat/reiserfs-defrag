/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <iostream>
#include <algorithm>

FsBitmap::FsBitmap(FsJournal *journal_, const FsSuperblock *sb_)
{
    this->journal = journal_;
    this->sb = sb_;
    uint32_t bitmap_block_count = (sb->s_block_count - 1) / BLOCKS_PER_BITMAP + 1;
    if (bitmap_block_count != sb->s_bmap_nr) {
        std::cout << "error: sb->s_bmap_nr doesn't correspond to filesystem size" << std::endl;
        // TODO: add error handling, exception would be fine
    }

    bitmap_blocks.resize(bitmap_block_count);

    for (uint32_t bitmap_idx = 0; bitmap_idx < bitmap_block_count; bitmap_idx ++) {
        uint32_t actual_block_idx = bitmap_idx * BLOCKS_PER_BITMAP;
        if (0 == actual_block_idx) actual_block_idx = FIRST_BITMAP_BLOCK;
        this->journal->readBlock(this->bitmap_blocks[bitmap_idx], actual_block_idx);
    }
}

FsBitmap::~FsBitmap()
{
}

uint32_t
FsBitmap::AGSize(uint32_t ag) const
{
    assert1 (ag < this->AGCount());
    if (this->AGCount() - 1 == ag) { // last AG
        const uint32_t rem = this->sb->s_block_count % this->ag_size;
        if (0 == rem)
            return this->ag_size;
        else
            return rem;
    } else {    // all other AGs
        return this->ag_size;
    }
}

uint32_t
FsBitmap::AGBegin(uint32_t ag) const
{
    assert1 (ag < this->AGCount());
    return ag * this->ag_size;
}

uint32_t
FsBitmap::AGEnd(uint32_t ag) const
{
    assert1 (ag < this->AGCount());
    const uint32_t agend = (ag + 1) * this->ag_size - 1;
    if (agend > this->sb->s_block_count - 1)
        return this->sb->s_block_count - 1;
    else
        return agend;
}

bool
FsBitmap::blockIsBitmap(uint32_t block_idx) const
{
    if (block_idx == FIRST_BITMAP_BLOCK)
        return true;
    if ((block_idx/BLOCKS_PER_BITMAP)*BLOCKS_PER_BITMAP == block_idx)
        return true;
    return false;
}

bool
FsBitmap::blockIsJournal(uint32_t block_idx) const
{
    uint32_t journal_start = this->sb->jp_journal_1st_block;
    // journal has one additional block for its 'header'
    uint32_t journal_end = journal_start + (this->sb->jp_journal_size - 1) + 1;
    return (journal_start <= block_idx) && (block_idx <= journal_end);
}

bool
FsBitmap::blockIsFirst64k(uint32_t block_idx) const
{
    return block_idx < 65536/BLOCKSIZE;
}

bool
FsBitmap::blockIsSuperblock(uint32_t block_idx) const
{
    return block_idx == SUPERBLOCK_BLOCK;
}

bool
FsBitmap::blockReserved(uint32_t block_idx) const
{
    return blockIsBitmap(block_idx) || blockIsJournal(block_idx) || blockIsFirst64k(block_idx)
        || blockIsSuperblock(block_idx);
}

void
FsBitmap::markBlockUsed(uint32_t block_idx)
{
    uint32_t bitmap_block_idx = block_idx / BLOCKS_PER_BITMAP;
    uint32_t inblock_bit_idx = block_idx % BLOCKS_PER_BITMAP;
    uint32_t inblock_dword_idx = inblock_bit_idx / 32;
    uint32_t indword_idx = inblock_bit_idx % 32;
    Block &bb = this->bitmap_blocks[bitmap_block_idx];
    uint32_t *c = reinterpret_cast<uint32_t *>(bb.buf + 4 * inblock_dword_idx);

    *c = *c | (static_cast<uint32_t>(1) << indword_idx);
    bb.markDirty();
    // mark AG dirty
    this->ag_free_extents[block_idx / this->ag_size].need_update = true;
}

void
FsBitmap::markBlockFree(uint32_t block_idx)
{
    uint32_t bitmap_block_idx = block_idx / BLOCKS_PER_BITMAP;
    uint32_t inblock_bit_idx = block_idx % BLOCKS_PER_BITMAP;
    uint32_t inblock_dword_idx = inblock_bit_idx / 32;
    uint32_t indword_idx = inblock_bit_idx % 32;
    Block &bb = this->bitmap_blocks[bitmap_block_idx];
    uint32_t *c = reinterpret_cast<uint32_t *>(bb.buf + 4 * inblock_dword_idx);

    *c = *c & ~(static_cast<uint32_t>(1) << indword_idx);
    bb.markDirty();
    // mark AG dirty
    this->ag_free_extents[block_idx / this->ag_size].need_update = true;
}

void
FsBitmap::markBlock(uint32_t block_idx, bool used)
{
    if (used)
        this->markBlockUsed(block_idx);
    else
        this->markBlockFree(block_idx);
}

bool
FsBitmap::blockUsed(uint32_t block_idx) const
{
    uint32_t bitmap_block_idx = block_idx / BLOCKS_PER_BITMAP;
    uint32_t inblock_bit_idx = block_idx % BLOCKS_PER_BITMAP;
    uint32_t inblock_byte_idx = inblock_bit_idx / 8;
    uint32_t inbyte_idx = inblock_bit_idx % 8;

    const Block &bb = this->bitmap_blocks[bitmap_block_idx];
    const uint8_t &c = reinterpret_cast<const uint8_t&>(bb.buf[inblock_byte_idx]);

    // result will be converted to bool automatically
    return c & (static_cast<uint8_t>(1) << inbyte_idx);
}

void
FsBitmap::writeChangedBitmapBlocks()
{
    for (std::vector<Block>::iterator it = this->bitmap_blocks.begin();
        it != this->bitmap_blocks.end(); ++ it)
    {
        if (it->dirty)
            this->journal->writeBlock(&*it);
    }
}

void
FsBitmap::setAGSize(uint32_t size)
{
    assert1 (size == AG_SIZE_128M || size == AG_SIZE_256M || size == AG_SIZE_512M);
    this->ag_size = size;
    uint32_t ag_count = (this->sizeInBlocks() - 1) / size + 1;
    this->ag_free_extents.clear();
    this->ag_free_extents.resize(ag_count);
    // AG configuration changed, need to rescan for free extents
    this->updateAGFreeExtents();
}

void
FsBitmap::updateAGFreeExtents()
{
    for (uint32_t ag = 0; ag < this->AGCount(); ag ++) {
        if (this->ag_free_extents[ag].need_update)
            this->rescanAGForFreeExtents(ag);
    }
}

static struct compare_by_extent_length {
    bool operator() (const FsBitmap::extent_t a, const FsBitmap::extent_t b) { return a.len > b.len; }
} compare_by_extent_length_obj;

int
FsBitmap::allocateFreeExtent(uint32_t &ag, uint32_t required_size,
                             std::vector<uint32_t> &blocks, uint32_t forbidden_ag)
{
    ag = ag % this->AGCount();  // [0, AGCount()-1]
    uint32_t start_ag = ag;
    do {
        if (forbidden_ag == ag) {   // avoid forbidden ag
            ag = (ag + 1) % this->AGCount();    // next
            continue;
        }
        ag_entry &fe = this->ag_free_extents[ag];
        uint32_t k = 0;
        while (k < fe.size() && fe[k].len >= required_size) k ++;
        if (k > 0) {    // there was least one appropriate extent:
            k --;       // previous, use it
            assert1 (k < fe.size());   // k must point to some element in vector
            assert2 ("extent should be large enough", fe[k].len >= required_size);
            blocks.clear();
            // fill blocks vector, decreasing extent
            while (required_size > 0) {
                blocks.push_back(fe[k].start);
                fe[k].start ++;
                fe[k].len --;
                required_size --;
            }
            // length must stay non-negative
            assert1 ((fe[k].len & 0x80000000) == 0);  // catch overflow, as .len unsigned
            // if we used whole extent, its length is zero, and it should be removed
            if (0 == fe[k].len) {
                fe.list.erase(fe.list.begin() + k);
            }
            // sort by length
            std::sort (fe.list.begin(), fe.list.end(), compare_by_extent_length_obj);

            return RFSD_OK;
        }
        ag = (ag + 1) % this->AGCount();    // proceed with next, wrap is necessary
    } while (ag != start_ag);

    return RFSD_FAIL;
}

void
FsBitmap::rescanAGForFreeExtents(uint32_t ag)
{
    const uint32_t block_start = this->AGBegin(ag);
    const uint32_t block_end = this->AGEnd(ag);

    this->ag_free_extents[ag].clear();
    this->ag_free_extents[ag].used_blocks = this->AGSize(ag);
    // find first empty block
    uint32_t ptr = block_start;
    do {
        while (ptr <= block_end && this->blockUsed(ptr)) ptr++;
        if (ptr > block_end)    // exit if there is no any
            break;

        extent_t ex;
        ex.start = ptr; ex.len = 0;
        while (ptr <= block_end && not this->blockUsed(ptr)) {
            ex.len ++;
            ptr++;
        }
        this->ag_free_extents[ag].push_back(ex);
        this->ag_free_extents[ag].used_blocks -= ex.len;
    } while (1);
    this->ag_free_extents[ag].need_update = false;

    // sort by extent length, large
    std::sort (this->ag_free_extents[ag].list.begin(), this->ag_free_extents[ag].list.end(),
        compare_by_extent_length_obj);

    // As we subtracted free block count from total AG lengthexclude reserved blocks
    this->ag_free_extents[ag].used_blocks -= this->reservedBlockCount(ag);
}

uint32_t
FsBitmap::reservedBlockCount(uint32_t ag) const
{
    return this->reservedBlockCount(this->AGBegin(ag), this->AGEnd(ag));
}

uint32_t
FsBitmap::reservedBlockCount(uint32_t from, uint32_t to) const
{
    // TODO: use interval arithmetics to speed up following code
    uint32_t rc = 0;
    for (uint32_t k = from; k <= to; k ++) {
        if (this->blockReserved(k))
            rc ++;
    }

    return rc;
}

uint32_t
FsBitmap::AGFreeBlockCount(uint32_t ag) const
{
    assert1 (ag < this->AGCount());
    uint32_t free_count = 0;
    for (uint32_t k = 0; k < this->ag_free_extents[ag].size(); k ++)
        free_count += this->ag_free_extents[ag][k].len;

    return free_count;
}
