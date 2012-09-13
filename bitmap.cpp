#include "reiserfs.hpp"
#include <iostream>

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
    for (uint32_t k = 0; k < bitmap_block_count; k ++) {
        this->bitmap_blocks[k].attachJournal(this->journal);
    }

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
