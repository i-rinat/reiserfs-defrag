#include "reiserfs.hpp"
#include <iostream>
#include <algorithm>

FsBitmap::FsBitmap(FsJournal *journal_, const FsSuperblock *sb_)
{
    this->journal = journal_;
    this->sb = sb_;
    uint32_t bitmap_block_count = (sb->s_block_count - 1) / BLOCKS_PER_BITMAP + 1;
    if (bitmap_block_count != sb->s_bmap_nr) {
        std::cerr << "error: sb->s_bmap_nr doesn't correspond to filesystem size" << std::endl;
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

void
FsBitmap::markBlockUsed(uint32_t block_idx)
{
    uint32_t bitmap_block_idx = block_idx / BLOCKS_PER_BITMAP;
    uint32_t inblock_bit_idx = block_idx % BLOCKS_PER_BITMAP;
    uint32_t inblock_byte_idx = inblock_bit_idx / 8;
    uint32_t inbyte_idx = inblock_bit_idx % 8;
    Block &bb = this->bitmap_blocks[bitmap_block_idx];
    uint8_t &c = reinterpret_cast<uint8_t&>(bb.buf[inblock_byte_idx]);

    c = c | (static_cast<uint8_t>(1) << inbyte_idx);
    bb.markDirty();
}

void
FsBitmap::markBlockFree(uint32_t block_idx)
{
    uint32_t bitmap_block_idx = block_idx / BLOCKS_PER_BITMAP;
    uint32_t inblock_bit_idx = block_idx % BLOCKS_PER_BITMAP;
    uint32_t inblock_byte_idx = inblock_bit_idx / 8;
    uint32_t inbyte_idx = inblock_bit_idx % 8;
    Block &bb = this->bitmap_blocks[bitmap_block_idx];
    uint8_t &c = reinterpret_cast<uint8_t&>(bb.buf[inblock_byte_idx]);

    c = c & ~(static_cast<uint8_t>(1) << inbyte_idx);
    bb.markDirty();
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
    assert (size == AG_SIZE_128M || size == AG_SIZE_256M || size == AG_SIZE_512M);
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

bool
FsBitmap::allocateFreeExtent(uint32_t &ag, uint32_t required_size,
                             std::vector<uint32_t> &blocks)
{
    uint32_t start_ag = ag;
    do {
        ag_entry &fe = this->ag_free_extents[ag];
        uint32_t k = 0;
        while (k < fe.size() && fe[k].len >= required_size) k ++;
        if (k > 0) {    // there was least one appropriate extent:
            k --;       // previous, use it
            assert (0 <= k && k < fe.size());   // k mus point to some element in vector
            assert (fe[k].len >= required_size); // ensure extent is large enough
            blocks.clear();
            // fill blocks vector, decreasing extent
            while (required_size > 0) {
                blocks.push_back(fe[k].start);
                fe[k].start ++;
                fe[k].len --;
                required_size --;
            }
            assert (fe[k].len >= 0);    // length must stay non-negative
            // if we used whole extent, its length is zero, and it should be removed
            if (0 == fe[k].len) {
                fe.list.erase(fe.list.begin() + k);
            }
            // sort by length
            std::sort (fe.list.begin(), fe.list.end(), compare_by_extent_length_obj);

            return true;
        }
        ag = (ag + 1) % this->AGCount();    // proceed with next, wrap is necessary
    } while (ag != start_ag);

    return false;
}

void
FsBitmap::rescanAGForFreeExtents(uint32_t ag)
{
    const uint32_t block_start = ag * this->ag_size;
    const uint32_t block_end = (ag + 1) * this->ag_size - 1;

    this->ag_free_extents[ag].clear();
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
    } while (1);
    this->ag_free_extents[ag].need_update = false;

    // sort by extent length, large
    std::sort (this->ag_free_extents[ag].list.begin(), this->ag_free_extents[ag].list.end(),
        compare_by_extent_length_obj);
}
