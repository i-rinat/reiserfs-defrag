#include "reiserfs.hpp"
#include <assert.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <set>


Defrag::Defrag(ReiserFs &fs) : fs(fs)
{
    this->desired_extent_length = 2048;
}

void
Defrag::treeThroughDefrag(uint32_t batch_size)
{
    std::vector<uint32_t> leaves;
    movemap_t movemap;
    Block::key_t last_key;
    Block::key_t start_key = Block::zero_key;
    uint32_t free_idx = this->nextTargetBlock(0);
    assert (free_idx != 0);

    // compute max batch size. Should reserve leaf block, with largest indirect item
    // (1012 pointers), plus leaf block itself, plus one block (prevent free_idx becoming zero)
    uint32_t max_batch_size = this->fs.freeBlockCount() - 1012 - 1 - 1;

    if (batch_size > max_batch_size)
        batch_size = max_batch_size;
    std::cout << "batch size = " << batch_size << " blocks" << std::endl;
    if (batch_size < 32) {
        std::cout << "batch_size too small" << std::endl;
        return;
    }

    // pack internal nodes first
    do {
        uint32_t old_free_idx = free_idx;
        std::cout << "--- packing internal nodes -----------" << std::endl;
        std::vector<ReiserFs::tree_element> internal_nodes;

        this->fs.enumerateInternalNodes(internal_nodes);
        movemap.clear();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert (free_idx != 0);
        }
        if (movemap.size() == 0)    // don't need to cleanup if all internal nodes in their places
            break;                  // already
        this->fs.cleanupRegionMoveDataDown(old_free_idx, free_idx - 1);
        free_idx = old_free_idx;

        this->fs.enumerateInternalNodes(internal_nodes);
        movemap.clear();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert (free_idx != 0);
        }
        this->fs.moveBlocks(movemap);
    } while (0);

    // process leaves and unformatted blocks
    while (1) {
        std::cout << "--------------------------------------" << std::endl;
        this->fs.enumerateLeaves(start_key, batch_size, leaves, last_key);
        if (leaves.size() == 0)     // nothing left
            break;
        uint32_t old_free_idx = free_idx;
        this->createMovemapFromListOfLeaves(movemap, leaves, free_idx);
        if (movemap.size() == 0) {
            start_key = last_key;
            continue;
        }
        this->fs.cleanupRegionMoveDataDown(old_free_idx, free_idx - 1);

        free_idx = old_free_idx;
        this->fs.enumerateLeaves(start_key, batch_size, leaves, last_key);
        if (leaves.size() == 0)     // nothing left
            break;
        this->createMovemapFromListOfLeaves(movemap, leaves, free_idx);
        std::cout << "movemap size = " << movemap.size() << std::endl;
        this->fs.moveBlocks(movemap);
        start_key = last_key;
    }
    std::cout << "data moving complete" << std::endl;
}

void
Defrag::createMovemapFromListOfLeaves(movemap_t &movemap, const std::vector<uint32_t> &leaves,
                                      uint32_t &free_idx)
{
    movemap.clear();
    for (std::vector<uint32_t>::const_iterator it = leaves.begin(); it != leaves.end(); ++ it) {
        uint32_t leaf_idx = *it;
        if (leaf_idx != free_idx)
            movemap[leaf_idx] = free_idx;
        free_idx = this->nextTargetBlock(free_idx);
        assert (free_idx != 0);
        Block *block_obj = this->fs.readBlock(leaf_idx);
        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (uint32_t idx = 0; idx < ih.length / 4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                if (0 == child_idx)     // sparse file
                    continue;
                if (child_idx != free_idx)
                    movemap[child_idx] = free_idx;
                free_idx = this->nextTargetBlock(free_idx);
                assert (free_idx != 0);
            }
        }
        this->fs.releaseBlock(block_obj);
    }
}

uint32_t
Defrag::nextTargetBlock(uint32_t previous)
{
    uint32_t fs_size = this->fs.sizeInBlocks();
    uint32_t next = previous + 1;
    while ((next < fs_size) && this->fs.blockReserved(next)) { next ++; }
    if (next < fs_size) return next;
    else return 0; // no one found
}

int
Defrag::prepareDefragTask(std::vector<uint32_t> &blocks, movemap_t &movemap)
{
    movemap.clear();    // may not be empty, need to clear
    if (blocks.size() == 0) // zero-length file is defragmented already
        return RFSD_OK;

    std::vector<FsBitmap::extent_t> extents;
    this->convertBlocksToExtents(blocks, extents);

    if (extents.size() <= 1)    // no need to defragment file with only one extent
        return RFSD_OK;

    // get ideal extent distribution
    std::vector<uint32_t> lengths;
    this->getDesiredExtentLengths(extents, lengths, 2048);

    //  b_begin      b_end (points to the next block after extent last one)
    //    ↓           ↓
    //  b |===========|=======|=|===================|
    //  c |====|====|====|====|====|====|====|====|=|
    //         ↑    ↑
    //     c_begin  c_end

    std::vector<uint32_t> free_blocks;
    std::vector<FsBitmap::extent_t>::const_iterator b_cur = extents.begin();
    std::vector<uint32_t>::const_iterator c_cur = lengths.begin();
    uint32_t b_begin = 0;
    uint32_t b_end = 0;
    uint32_t c_begin = 0;
    uint32_t c_end = 0;

    uint32_t ag = blocks[0] / this->fs.bitmap->AGSize();

    while (c_cur != lengths.end()) {
        if (c_end < b_end) {
            c_begin = c_end;
            c_end = c_begin + *c_cur;
            // defragment if [c_begin, c_end-1] ⊈ [b_begin, b_end-1]
            if (b_begin > c_begin || c_end > b_end) {
                const uint32_t c_len = c_end - c_begin;
                if (RFSD_OK == this->fs.bitmap->allocateFreeExtent(ag, c_len, free_blocks)) {
                    for (uint32_t k = c_begin; k < c_end; k ++) {
                        movemap[blocks[k]] = free_blocks[k - c_begin];
                    }
                    this->success_count ++;
                } else {
                    this->failure_count ++;
                }
            }
            c_cur ++;
        } else {
            b_begin = b_end;
            b_end = b_begin + b_cur->len;
            b_cur ++;
        }
    }

    return RFSD_OK;
}

uint32_t
Defrag::getDesiredExtentLengths(const std::vector<FsBitmap::extent_t> &extents,
                                std::vector<uint32_t> &lengths, uint32_t target_length)
{
    target_length = std::max(128u, target_length);
    uint32_t total_length = 0;
    for (uint32_t k = 0; k < extents.size(); k ++) total_length += extents[k].len;

    uint32_t remaining = total_length;
    lengths.clear();
    while (remaining > target_length) {
        lengths.push_back(target_length);
        remaining -= target_length;
    }
    if (remaining > 0) lengths.push_back(remaining);

    return total_length;
}

void
Defrag::filterOutSparseBlocks(std::vector<uint32_t> &blocks)
{
    std::vector<uint32_t>::iterator iter_front = blocks.begin();
    std::vector<uint32_t>::iterator iter_back = blocks.begin();

    for (; iter_front != blocks.end(); ++ iter_front) {
        if (0 != *iter_front) {
            *iter_back = *iter_front;
            iter_back ++;
        }
    }
    blocks.erase(iter_back, blocks.end());
}

void
Defrag::convertBlocksToExtents(const std::vector<uint32_t> &blocks,
                               std::vector<FsBitmap::extent_t> &extents)
{
    FsBitmap::extent_t ex;

    extents.clear();
    if (0 == blocks.size())         // no blocks -- no extents
        return;
    ex.start = blocks[0];
    ex.len = 1;
    for (uint32_t k = 1; k < blocks.size(); k ++) {
        if (0 == blocks[k])     // skip sparse blocks
            continue;
        if (ex.start + (ex.len - 1) + 1 == blocks[k]) {
            // next block extends current extent
            ex.len ++;
        } else {
            // remember current extent and start another
            extents.push_back(ex);
            ex.start = blocks[k];
            ex.len = 1;
        }
    }
    extents.push_back(ex);      // push last extent
}

int
Defrag::mergeMovemap(movemap_t &dest, const movemap_t &src)
{
    const uint32_t prev_dest_size = dest.size();

    dest.insert(src.begin(), src.end());
    // if new dest size equals to sum of previous dest and src sizes, there was
    if (dest.size() == prev_dest_size + src.size())     // no intersections.
        return RFSD_OK;
    else
        return RFSD_FAIL;   // Otherwise they was. And it's bad.
}

int
Defrag::freeOneAG()
{
    // randomly select AG to sweep out
    const uint32_t offset = rand() % fs.bitmap->AGCount();
    for (uint32_t k = 0; k < fs.bitmap->AGCount(); k ++) {
        uint32_t ag = (k + offset) % fs.bitmap->AGCount();
        if (fs.bitmap->AGUsedBlockCount(ag) < fs.bitmap->AGSize()/2) {
            if (RFSD_OK == fs.sweepOutAG(ag))
                return RFSD_OK;
        }
    }
    return RFSD_FAIL;
}

int
Defrag::squeezeAllAGsWithThreshold(uint32_t threshold)
{
    uint32_t ags_to_squeeze = 0;
    for (uint32_t ag = 0; ag < this->fs.bitmap->AGCount(); ag ++) {
        if (this->fs.bitmap->AGExtentCount(ag) > threshold)
            ags_to_squeeze ++;
    }

    Progress progress(ags_to_squeeze);
    progress.setName("[squeeze]");
    for (uint32_t ag = 0; ag < this->fs.bitmap->AGCount(); ag ++) {
        if (this->fs.bitmap->AGExtentCount(ag) > threshold) {
            if (RFSD_FAIL == this->fs.squeezeDataBlocksInAG(ag)) {
                progress.abort();
                return RFSD_FAIL;
            }
            progress.inc();
        }
    }
    progress.show100();
    return RFSD_OK;
}

int
Defrag::experimental_v2()
{
    std::vector<uint32_t> leaves;
    Block::key_t start_key = Block::zero_key;
    Block::key_t next_key;
    blocklist_t file_blocks;
    movemap_t movemap;
    Progress progress;

    // estimate run time
    uint32_t obj_count = 0;
    while (1) {
        fs.getLeavesOfObject(start_key, next_key, leaves);
        if (next_key.sameObjectAs(start_key)) break;
        obj_count ++;
        start_key = next_key;
    }

    start_key = Block::zero_key;
    progress.setMaxValue(obj_count);
    progress.setName("[incremental]");

    while (1) {
        fs.getLeavesOfObject(start_key, next_key, leaves);
        progress.inc();

        bool first_indirect_of_file = true;
        file_blocks.clear();
        for (blocklist_t::iterator it = leaves.begin(); it != leaves.end(); ++ it) {
            Block *block_obj = fs.readBlock(*it);
            bool first_indirect_in_leaf = true;
            for (uint32_t item = 0; item < block_obj->itemCount(); item ++) {
                const Block::item_header &ih = block_obj->itemHeader(item);
                if (not ih.key.sameObjectAs(start_key)) continue;
                if (KEY_TYPE_INDIRECT == ih.type()) {
                    // only add leaf block if first indirect item refers to current file
                    // but avoid adding leaf with very first indirect item
                    if (first_indirect_in_leaf && !first_indirect_of_file)
                        file_blocks.push_back(*it);
                    for (uint32_t idx = 0; idx < ih.length / 4; idx ++) {
                        file_blocks.push_back(block_obj->indirectItemRef(ih.offset, idx));
                    }
                    first_indirect_in_leaf = false;
                    first_indirect_of_file = false;
                }
            }
            fs.releaseBlock(block_obj);
        }

        this->filterOutSparseBlocks(file_blocks);
        if (0 != file_blocks.size()) {
            movemap_t partial_movemap;
            if (RFSD_FAIL == this->prepareDefragTask(file_blocks, partial_movemap)) {
                std::cout << "temporal failure" << std::endl;
                // we get here if free extent allocation failed. That may mean we have too
                // fragmented free space. So try to free one of the AG.
                if (RFSD_FAIL == this->freeOneAG())
                    return RFSD_FAIL;
                if (RFSD_FAIL == this->prepareDefragTask(file_blocks, partial_movemap))
                    return RFSD_FAIL;
            }
            this->mergeMovemap(movemap, partial_movemap);
            if (movemap.size() > 8000) {
                fs.moveBlocks(movemap);
                movemap.clear();
            }
        }

        // next_key's and start_key's reference to the same object means we are done
        // with tree enumeration and may exit
        if (next_key.sameObjectAs(start_key))
            break;
        start_key = next_key;
    }

    if (movemap.size() > 0) {
        fs.moveBlocks(movemap);
        movemap.clear();
    }
    progress.show100();

    return RFSD_OK;
}
