/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <set>


Defrag::Defrag(ReiserFs &fs) : fs(fs)
{
    this->desired_extent_length = 2048;
    this->previous_obj_count = 0;
}

int
Defrag::DefragTreeThrough(uint32_t batch_size)
{
    std::vector<uint32_t> leaves;
    movemap_t movemap;
    Block::key_t last_key;
    Block::key_t start_key;
    uint32_t free_idx = this->nextTargetBlock(0);
    assert1 (free_idx != 0);

    // compute max batch size. Should reserve leaf block, with largest indirect item
    // (1012 pointers), plus leaf block itself, plus one block (prevent free_idx becoming zero)
    uint32_t max_batch_size = this->fs.freeBlockCount() - 1012 - 1 - 1;

    if (batch_size > max_batch_size)
        batch_size = max_batch_size;

    if (batch_size < 32) {
        std::cout << "batch_size too small" << std::endl;
        return RFSD_FAIL;
    }

    // pack internal nodes first
    do {
        Progress progress_internal_nodes;
        progress_internal_nodes.setMaxValue(4);
        progress_internal_nodes.setName("[packing internal nodes]");
        uint32_t old_free_idx = free_idx;
        std::vector<ReiserFs::tree_element> internal_nodes;

        this->fs.enumerateInternalNodes(internal_nodes);
        movemap.clear();
        progress_internal_nodes.inc();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert1 (free_idx != 0);
        }
        if (movemap.size() == 0)    // don't need to cleanup if all internal nodes in their places
            break;                  // already
        this->fs.cleanupRegionMoveDataDown(old_free_idx, free_idx - 1);
        progress_internal_nodes.inc();
        free_idx = old_free_idx;

        this->fs.enumerateInternalNodes(internal_nodes);
        progress_internal_nodes.inc();
        movemap.clear();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert1 (free_idx != 0);
        }
        this->fs.moveBlocks(movemap);
        progress_internal_nodes.show100();
    } while (0);

    // estimate work amount
    start_key = Block::zero_key;
    uint32_t work_amount = 0;
    Progress estimation;
    estimation.enableUnknownMode(true, 1000);
    estimation.setName("[estimate]");
    while (1) {
        this->fs.enumerateLeaves(start_key, batch_size, leaves, last_key);
        if (leaves.size() == 0)     // nothing left
            break;
        start_key = last_key;
        work_amount += leaves.size();
        estimation.update(work_amount);
        if (ReiserFs::userAskedForTermination()) {
            estimation.abort();
            return RFSD_FAIL;
        }
    }

    // process leaves and unformatted blocks
    start_key = Block::zero_key;
    Progress progress;
    progress.setMaxValue(work_amount);
    progress.setName("[treethrough]");
    progress.update(0);
    while (1) {
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
        this->fs.moveBlocks(movemap);
        start_key = last_key;
        if (ReiserFs::userAskedForTermination()) {
            progress.abort();
            return RFSD_FAIL;
        }
        progress.inc(leaves.size());
    }
    progress.show100();

    return RFSD_OK;
}

int
Defrag::DefragPath(uint32_t batch_size)
{
    std::vector<uint32_t> leaves;
    movemap_t movemap;
    Block::key_t last_key;
    Block::key_t start_key;
    uint32_t free_idx = this->nextTargetBlock(0);
    assert1 (free_idx != 0);

    // compute max batch size. Should reserve leaf block, with largest indirect item
    // (1012 pointers), plus leaf block itself, plus one block (prevent free_idx becoming zero)
    uint32_t max_batch_size = this->fs.freeBlockCount() - 1012 - 1 - 1;

    if (batch_size > max_batch_size)
        batch_size = max_batch_size;

    if (batch_size < 32) {
        std::cout << "batch_size too small" << std::endl;
        return RFSD_FAIL;
    }

    // pack internal nodes first
    do {
        Progress progress_internal_nodes;
        progress_internal_nodes.setMaxValue(4);
        progress_internal_nodes.setName("[packing internal nodes]");
        uint32_t old_free_idx = free_idx;
        std::vector<ReiserFs::tree_element> internal_nodes;

        this->fs.enumerateInternalNodes(internal_nodes);
        movemap.clear();
        progress_internal_nodes.inc();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert1 (free_idx != 0);
        }
        if (movemap.size() == 0)    // don't need to cleanup if all internal nodes in their places
            break;                  // already
        this->fs.cleanupRegionMoveDataDown(old_free_idx, free_idx - 1);
        progress_internal_nodes.inc();
        free_idx = old_free_idx;

        this->fs.enumerateInternalNodes(internal_nodes);
        progress_internal_nodes.inc();
        movemap.clear();
        for (std::vector<ReiserFs::tree_element>::iterator it = internal_nodes.begin();
            it != internal_nodes.end(); ++ it)
        {
            uint32_t int_node_idx = it->idx;
            if (int_node_idx !=  free_idx)
                movemap[int_node_idx] = free_idx;
            free_idx = this->nextTargetBlock(free_idx);
            assert1 (free_idx != 0);
        }
        this->fs.moveBlocks(movemap);
        progress_internal_nodes.show100();
    } while (0);

    return RFSD_OK;
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
        assert1 (free_idx != 0);
        Block *block_obj = this->fs.readBlock(leaf_idx);
        block_obj->checkLeafNode();
        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (uint32_t idx = 0; idx < ih.length / 4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih, idx);
                if (0 == child_idx)     // sparse file
                    continue;
                if (child_idx != free_idx)
                    movemap[child_idx] = free_idx;
                free_idx = this->nextTargetBlock(free_idx);
                assert1 (free_idx != 0);
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

bool
Defrag::objectIsSealed(const Block::key_t &k) const
{
    const Block::key_t k_z(KEY_V0, k.dir_id, k.obj_id, 0, 0);
    return (this->sealed_objs.count(k) > 0);
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

    uint32_t ag = this->fs.bitmap->AGOfBlock(blocks[0]);

    bool some_extents_failed = false;
    bool some_extents_succeeded = false;
    bool some_extents_touched = false;
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
                    some_extents_succeeded = true;
                } else {
                    some_extents_failed = true;
                }
                some_extents_touched = true;
            }
            c_cur ++;
        } else {
            b_begin = b_end;
            b_end = b_begin + b_cur->len;
            b_cur ++;
        }
    }

    if (!some_extents_touched)      // all extents already defragmented
        return RFSD_OK;

    if (some_extents_succeeded) {
        if (some_extents_failed)
            this->defrag_statistics.partial_success_count ++;
        else
            this->defrag_statistics.success_count ++;

        this->defrag_statistics.total_count ++;
        return RFSD_OK;
    }

    this->defrag_statistics.failure_count ++;
    this->defrag_statistics.total_count ++;
    return RFSD_FAIL;
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
Defrag::moveObjectsUp(const std::vector<Block::key_t> &objs)
{
    uint32_t next_ag = 0;
    uint32_t free_blocks_count = 0;
    uint32_t blocks_moved = 0;
    uint32_t files_moved = 0;
    uint32_t free_idx = 0;
    uint32_t work_amount = 0;
    uint32_t limit = 15*2048;   // limit block count for getIndirectBlocksOfObject
    movemap_t movemap;

    std::cout << "moving " << objs.size() << " file(s) up" << std::endl;
    Progress estimation;
    estimation.setName("[estimate]");
    estimation.enableUnknownMode(true, 100);
    for (std::vector<Block::key_t>::const_iterator it = objs.begin(); it != objs.end(); ++ it) {
        uint32_t next_offset, start_offset = 0;
        Block::key_t next_key, start_key = *it;
        blocklist_t file_blocks;

        do {
            fs.getIndirectBlocksOfObject(start_key, start_offset, next_key, next_offset,
                                         file_blocks, limit);
            work_amount += file_blocks.size();
            start_key = next_key;
            start_offset = next_offset;
        } while (it->sameObjectAs(next_key));

        estimation.update(work_amount);
        if (ReiserFs::userAskedForTermination()) {
            estimation.abort();
            return RFSD_FAIL;
        }
    }

    Progress moveup_progress(work_amount);
    moveup_progress.setName("[moving files up]");
    moveup_progress.update(0);
    for (std::vector<Block::key_t>::const_iterator it = objs.begin(); it != objs.end(); ++ it) {
        uint32_t start_offset = 0;
        uint32_t next_offset;

        Block::key_t next_key;
        Block::key_t start_key = *it;
        blocklist_t file_blocks;

        do {
            fs.getIndirectBlocksOfObject(start_key, start_offset, next_key, next_offset,
                                         file_blocks, limit);
            if (file_blocks.size() > free_blocks_count) {
                // no space for current file, let's free some.
                // But before we must flush movemap
                fs.moveBlocks(movemap);
                movemap.clear();
                if (ReiserFs::userAskedForTermination()) {
                    moveup_progress.abort();
                    return RFSD_FAIL;
                }

                fs.sweepOutAG(next_ag);
                fs.sealAG(next_ag);
                free_blocks_count += fs.bitmap->AGFreeBlockCount(next_ag);
                next_ag ++;
                if (next_ag >= fs.bitmap->AGCount()) {
                    std::cout << "warning: insufficient free space for file packing" << std::endl;
                    return RFSD_FAIL;
                }
                // need get blocks again as sweep could change their positions
                fs.getIndirectBlocksOfObject(start_key, start_offset, next_key, next_offset,
                                              file_blocks, limit);
            }

            const uint32_t progress_update = file_blocks.size();
            this->filterOutSparseBlocks(file_blocks);

            start_key = next_key;
            start_offset = next_offset;

            for (uint32_t k = 0; k < file_blocks.size(); k ++) {
                free_idx = fs.findFreeBlockAfter(free_idx);
                assert1(free_idx != 0);
                movemap[file_blocks[k]] = free_idx;
                blocks_moved ++;
                free_blocks_count --;
            }
            moveup_progress.inc(progress_update);

            if (movemap.size() > 8000) {
                fs.moveBlocks(movemap);
                movemap.clear();
                if (ReiserFs::userAskedForTermination()) {
                    moveup_progress.abort();
                    return RFSD_FAIL;
                }
            }
        } while (it->sameObjectAs(next_key));

        if (file_blocks.size() > 0) files_moved ++;
    }

    // move remaining
    fs.moveBlocks(movemap);
    movemap.clear();
    moveup_progress.show100();
    std::cout << blocks_moved << " block(s) of " << files_moved << " file(s) moved up" << std::endl;
    return RFSD_OK;
}

int
Defrag::freeOneAG()
{
    // select most freespace fragmented AG to sweep out.
    // Additional random which gives a chance to low fragmented AG to be selected.
    // It should be limited though, in order to heavily fragmented AG to cleared first
    uint32_t max_score = 0;
    uint32_t selected_ag = ~0u;

    for (uint32_t ag = 0; ag < fs.bitmap->AGCount(); ag ++) {
        if (!fs.AGSealed(ag)) {
            selected_ag = ag;
            break;
        }
    }

    if (~0u == selected_ag)      // every AG sealed, can not do anything with it. Give up.
        return RFSD_FAIL;

    for (uint32_t ag = 0; ag < fs.bitmap->AGCount(); ag ++) {
        if (fs.AGSealed(ag))    // skip sealed AGs
            continue;
        const uint32_t score = 128 * fs.bitmap->AGExtentCount(ag) + rand() % 1024;
        if (score > max_score) {
            max_score = score;
            selected_ag = ag;
        }
    }

    if (RFSD_FAIL == fs.sweepOutAG(selected_ag))
        return RFSD_FAIL;
    return RFSD_OK;
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
        if (ReiserFs::userAskedForTermination()) {
            progress.abort();
            return RFSD_FAIL;
        }
    }
    progress.show100();
    return RFSD_OK;
}

int
Defrag::DefragIncremental(uint32_t batch_size, bool use_previous_estimation)
{
    Block::key_t start_key = Block::zero_key;
    Block::key_t next_key;
    blocklist_t file_blocks;
    movemap_t movemap;
    uint32_t start_offset;
    uint32_t next_offset;
    uint32_t limit = 15*2048;
    uint32_t obj_count = 0;


    if (use_previous_estimation && (0 != this->previous_obj_count)) {
        obj_count = this->previous_obj_count;
    } else {
        // estimate run time
        Progress estimation;
        estimation.enableUnknownMode(true, 10000);
        estimation.setName("[estimate]");

        start_offset = 0;
        while (1) {
            fs.getIndirectBlocksOfObject(start_key, start_offset, next_key, next_offset,
                                         file_blocks, limit);
            if (next_key.sameObjectAs(start_key) && (next_offset == 0)) break;
            obj_count ++;
            start_key = next_key;
            start_offset = next_offset;
            estimation.inc();
            if (ReiserFs::userAskedForTermination()) {
                estimation.abort();
                return RFSD_FAIL;
            }
        }
        // save obj_count for consequent passes
        this->previous_obj_count = obj_count;
    }

    Progress progress;
    progress.setMaxValue(obj_count);
    progress.setName("[incremental]");
    start_key = Block::zero_key;
    start_offset = 0;
    this->defrag_statistics.reset();

    while (1) {
        if (ReiserFs::userAskedForTermination()) {
            progress.abort();
            this->showDefragStatistics();
            return RFSD_FAIL;
        }

        fs.getIndirectBlocksOfObject(start_key, start_offset, next_key, next_offset,
                                     file_blocks, limit);
        progress.inc();

        this->filterOutSparseBlocks(file_blocks);
        if (0 != file_blocks.size() && not this->objectIsSealed(start_key)) {
            movemap_t partial_movemap;
            if (RFSD_FAIL == this->prepareDefragTask(file_blocks, partial_movemap)) {
                // Before we do anything, we must process all pending moves as further moves may
                // lead to inconsistency.
                fs.moveBlocks(movemap);
                movemap.clear();
                // we get here if free extent allocation failed. That may mean we have too
                // fragmented free space. So try to free one of the AG.
                if (RFSD_FAIL == this->freeOneAG()) {
                    progress.abort();
                    this->showDefragStatistics();
                    return RFSD_FAIL;
                }
                continue;   // restart with current parameters
            }

            this->mergeMovemap(movemap, partial_movemap);
            if (movemap.size() > batch_size) {
                fs.moveBlocks(movemap);
                movemap.clear();
            }
        }

        // next_key's and start_key's reference to the same object means we are done
        // with tree enumeration and may exit
        if (next_key.sameObjectAs(start_key) && (next_offset == 0))
            break;
        start_key = next_key;
        start_offset = next_offset;
    }

    if (movemap.size() > 0) {
        fs.moveBlocks(movemap);
        movemap.clear();
    }

    progress.show100();
    this->showDefragStatistics();

    return RFSD_OK;
}

void
Defrag::sealObjects(const std::vector<Block::key_t> &objs)
{
    this->sealed_objs.clear();
    for (std::vector<Block::key_t>::const_iterator it = objs.begin(); it != objs.end(); ++ it) {
        this->sealed_objs.insert(*it);
    }
}

void
Defrag::showDefragStatistics()
{
    std::cout << "defrag statistics: ";
    std::cout << this->defrag_statistics.total_count << "/";
    std::cout << this->defrag_statistics.success_count << "/";
    std::cout << this->defrag_statistics.partial_success_count << "/";
    std::cout << this->defrag_statistics.failure_count;
    std::cout << " (total/success/partialsuccess/failure)" << std::endl;
}

uint32_t
Defrag::lastDefragImperfectCount() {
    return this->defrag_statistics.failure_count
           + this->defrag_statistics.partial_success_count;
}
