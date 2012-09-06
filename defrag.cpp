#include "reiserfs.hpp"
#include <assert.h>
#include <iostream>
#include <vector>

typedef std::map<uint32_t, uint32_t> movemap_t;

uint32_t nextTargetBlock(const ReiserFs &fs, uint32_t previous) {
    uint32_t fs_size = fs.sizeInBlocks();
    uint32_t next = previous + 1;
    while ((next < fs_size) && fs.blockIsReserved(next)) { next ++; }
    if (next < fs_size) return next;
    else return 0; // no one found
}

movemap_t *
createLargeScaleMovemap(const ReiserFs &fs)
{
    std::vector<ReiserFs::tree_element> *tree = fs.enumerateTree();
    std::vector<ReiserFs::tree_element>::const_iterator iter;
    movemap_t *movemap_ptr = new std::map<uint32_t, uint32_t>;
    movemap_t &movemap = *movemap_ptr;

    uint32_t free_idx = 0;

    // move all internal nodes to beginning of partition
    for (iter = tree->begin(); iter != tree->end(); ++ iter) {
        if (iter->type == BLOCKTYPE_INTERNAL) {
            free_idx = nextTargetBlock(fs, free_idx);
            assert (free_idx != 0);
            assert (movemap.count(iter->idx) == 0);
            movemap[iter->idx] = free_idx;
        }
    }

    for (iter = tree->begin(); iter != tree->end(); ++ iter) {
        if (iter->type == BLOCKTYPE_LEAF) {
            free_idx = nextTargetBlock(fs, free_idx);
            assert (free_idx != 0);
            movemap[iter->idx] = free_idx;
            Block *block_obj = fs.readBlock(iter->idx);
            for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
                const Block::item_header &ih = block_obj->itemHeader(k);
                if (ih.key.type(ih.version) != KEY_TYPE_INDIRECT) continue;

                for (uint32_t idx = 0; idx < ih.length/4; idx ++) {
                    uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                    free_idx = nextTargetBlock(fs, free_idx);
                    assert (free_idx != 0);
                    assert (movemap.count(child_idx) == 0);
                    movemap[child_idx] = free_idx;
                }
            }
            fs.releaseBlock(block_obj);
        }
    }
    delete tree;
    return movemap_ptr;
}

int
main (int argc, char *argv[])
{
    ReiserFs fs;
    fs.open("../image/reiserfs.image.shuffled");

    std::map<uint32_t, uint32_t> *movemap = createLargeScaleMovemap(fs);

    std::cout << "movemap size = " << movemap->size() << std::endl;
    delete movemap;

    fs.close();
    return 0;
}
