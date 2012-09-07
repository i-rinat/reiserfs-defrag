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

uint32_t
removeDegenerateEntries(movemap_t &movemap)
{
    movemap_t::iterator iter;
    movemap_t::iterator del_iter;
    uint32_t degenerate_count = 0;

    iter = movemap.begin();
    while (iter != movemap.end()) {
        del_iter = iter ++;
        if (del_iter->first == del_iter->second) {
            movemap.erase(del_iter);
            degenerate_count ++;
        }
    }
    return degenerate_count;
}

void
extractCleanMoves(const ReiserFs &fs, movemap_t &movemap, movemap_t &clean_moves)
{
    movemap_t::iterator iter;
    movemap_t::iterator wi;

    clean_moves.clear();
    iter = movemap.begin();
    while (iter != movemap.end()) {
        wi = iter ++;
        if (fs.blockUsed(wi->first) && !fs.blockIsReserved(wi->first) && !fs.blockUsed(wi->second)) {
            clean_moves[wi->first] = wi->second;
            movemap.erase(wi);
        }
    }
}

uint32_t
cleanupRegion(ReiserFs &fs, uint32_t from) {
    uint32_t to = (from/BLOCKS_PER_BITMAP + 1)*BLOCKS_PER_BITMAP;
    if (to > fs.sizeInBlocks()) to = fs.sizeInBlocks();
    uint32_t next_free = to;
    movemap_t movemap;

    uint32_t cur = from;
    while (cur < to) {
        if (!fs.blockIsReserved(cur) && fs.blockUsed(cur)) {
            next_free = fs.findFreeBlockAfter(next_free);
            assert (next_free != 0);
            movemap[cur] = next_free;
        }
        cur++;
    }
    std::cout << "cleanupRegion [" << from << ":" << to << "], " << movemap.size()
        << " elements" << std::endl;
    fs.moveMultipleBlocks(movemap);
    return to;
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

    uint32_t blocks_moved = 0;
    do {
        std::map<uint32_t, uint32_t> *movemap = createLargeScaleMovemap(fs);

        uint32_t cnt = removeDegenerateEntries(*movemap);
        std::cout << "degenerate moves removed = " << cnt << std::endl;
        std::cout << "movemap size = " << movemap->size() << std::endl;

        movemap_t clean_moves;
        extractCleanMoves(fs, *movemap, clean_moves);
        std::cout << "clean_moves size = " << clean_moves.size() << std::endl;

        blocks_moved = fs.moveMultipleBlocks(clean_moves);
        delete movemap;
    } while (blocks_moved > 0);

    fs.close();
    return 0;
}
