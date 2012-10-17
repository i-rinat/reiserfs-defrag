#include "reiserfs.hpp"
#include <assert.h>
#include <iostream>
#include <vector>
#include <set>

typedef ReiserFs::movemap_t movemap_t;

class Defrag {
public:
    Defrag (ReiserFs &fs);
    void treeThroughDefrag(uint32_t batch_size = 16000);
    void experimental_v1();

private:
    ReiserFs &fs;
    uint32_t desired_extent_length;

    uint32_t nextTargetBlock(uint32_t previous);
    void createMovemapFromListOfLeaves(movemap_t &movemap, const std::vector<uint32_t> &leaves,
                                       uint32_t &free_idx);
    /// try to defragment block list
    ///
    /// \param      blocks      block list
    /// \return RFSD_OK on partial success and RFSD_FAIL if all attempts failed
    int defragmentBlocks(std::vector<uint32_t> &blocks);
    uint32_t getDesiredExtentLengths(const std::vector<ReiserFs::extent_t> &extents,
                                     std::vector<uint32_t> &lengths, uint32_t target_length);
    void convertBlocksToExtents(const std::vector<uint32_t> &blocks,
                                std::vector<ReiserFs::extent_t> &extents);
    /// filters out sparse blocks from \param blocks by eliminating all zeros
    void filterOutSparseBlocks(std::vector<uint32_t> &blocks);
};

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
Defrag::defragmentBlocks(std::vector<uint32_t> &blocks)
{
    if (blocks.size() == 0) // zero-length file is defragmented already
        return RFSD_OK;

    std::vector<ReiserFs::extent_t> extents;
    this->convertBlocksToExtents(blocks, extents);

    if (extents.size() <= 1)    // no need to defragment file with only one extent
        return RFSD_OK;

    std::cout << "extents.size() == " << extents.size() << std::endl;
    for (uint32_t k = 0; k < extents.size(); k ++) {
        std::cout << extents[k].start << "(" << extents[k].len << ") " << std::endl;
    }

    // get ideal extent distribution
    std::vector<uint32_t> lengths;
    this->getDesiredExtentLengths(extents, lengths, 2048);
    // TODO: remove debug prints
    std::cout << "initial lengths: ";
    for (uint32_t k = 0; k < extents.size(); k ++) std::cout << extents[k].len << ", ";
    std::cout << std::endl;
    std::cout << "desired lengths: ";
    for (uint32_t k = 0; k < lengths.size(); k ++) std::cout << lengths[k] << ", ";
    std::cout << std::endl;
    // ======

    //  b_begin      b_end
    //    ↓           ↓
    //  b |===========|=======|=|===================|
    //  c |====|====|====|====|====|====|====|====|=|
    //         ↑    ↑
    //     c_begin  c_end

    movemap_t movemap;
    std::vector<uint32_t> free_blocks;
    std::vector<ReiserFs::extent_t>::const_iterator b_cur = extents.begin();
    std::vector<uint32_t>::const_iterator c_cur = lengths.begin();
    uint32_t b_begin = 0;
    uint32_t b_end = 0;
    uint32_t c_begin = 0;
    uint32_t c_end = 0;

    while (c_cur != lengths.end()) {
        if (c_end < b_end) {
            c_begin = c_end;
            c_end = c_begin + *c_cur;
            if (b_begin > c_begin || c_end > b_end) {
                uint32_t ag = 0;
                if (this->fs.allocateFreeExtent(ag, c_end - c_begin, free_blocks)) {
                    for (uint32_t k = c_begin; k < c_end; k ++) {
                        movemap[blocks[k]] = free_blocks[k - c_begin];
                    }
                }
            }
            c_cur ++;
        } else {
            b_begin = b_end;
            b_end = b_begin + b_cur->len;
            b_cur ++;
        }
    }

    std::cout << std::endl;

    std::cout << "movemap size = " << movemap.size() << std::endl;
    std::cout << this->fs.moveBlocks(movemap) << " blocks moved" << std::endl;

    return RFSD_OK;
}

uint32_t
Defrag::getDesiredExtentLengths(const std::vector<ReiserFs::extent_t> &extents,
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
                               std::vector<ReiserFs::extent_t> &extents)
{
    ReiserFs::extent_t ex;

    extents.clear();
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

void
Defrag::experimental_v1()
{
    std::vector<uint32_t> leaves;
    Block::key_t start_key = Block::zero_key;
    Block::key_t last_key = Block::zero_key;

    fs.enumerateLeaves(start_key, 1000000, leaves, last_key);
    std::cout << "leaves: " << leaves.size() << std::endl;

    if (leaves.size() > 0) {
        std::vector<uint32_t> file_blocks;
        struct {
            uint32_t dir_id;
            uint32_t obj_id;
        } current_obj;

        bool first_leaf_in_batch = true;
        std::vector<uint32_t>::const_iterator iter;
        for (iter = leaves.begin(); iter != leaves.end(); ++ iter) {
            const uint32_t leaf_idx = *iter;
            //std::cout << "leaf: " << leaf_idx << std::endl;

            Block *block_obj = fs.readBlock(leaf_idx);
            bool first_indirect_in_leaf = true;
            for (uint32_t item = 0; item < block_obj->itemCount(); item ++) {
                const Block::item_header &ih = block_obj->itemHeader(item);
                if (first_leaf_in_batch) {  // initialize one time per batch
                    current_obj.dir_id = ih.key.dir_id;
                    current_obj.obj_id = ih.key.obj_id;
                    first_leaf_in_batch = false;
                }
                if (current_obj.dir_id != ih.key.dir_id || current_obj.obj_id != ih.key.obj_id) {
                    // new file started, time to process previous one
                    // delete references to sparse blocks
                    this->filterOutSparseBlocks(file_blocks);
                    this->defragmentBlocks(file_blocks);
                    // prepare structures for new file
                    current_obj.dir_id = ih.key.dir_id;
                    current_obj.obj_id = ih.key.obj_id;
                    file_blocks.clear();
                }

                ih.key.dump(ih.version, std::cout, true);

                if (KEY_TYPE_INDIRECT == ih.type()) {
                    // only add leaf block if first indirect item refers to current file
                    if (first_indirect_in_leaf)
                        file_blocks.push_back(leaf_idx);
                    for (uint32_t idx = 0; idx < ih.length / 4; idx ++) {
                        file_blocks.push_back(block_obj->indirectItemRef(ih.offset, idx));
                    }
                    first_indirect_in_leaf = false;
                }
            }
            fs.releaseBlock(block_obj);
        } // for (leaves)

        this->defragmentBlocks(file_blocks);
    }
}

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    if (argc > 1) {
        if (RFSD_OK != fs.open(argv[1], false))
            return 1;
    } else {
        if (RFSD_OK != fs.open("../image/reiserfs.image", false))
            return 1;
    }
    fs.useDataJournaling(false);

    Defrag defrag(fs);
    // defrag.treeThroughDefrag(8000);
    defrag.experimental_v1();

    fs.close();
    return 0;
}
