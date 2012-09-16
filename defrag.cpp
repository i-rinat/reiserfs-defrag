#include "reiserfs.hpp"
#include <assert.h>
#include <iostream>
#include <vector>
#include <set>

typedef std::map<uint32_t, uint32_t> movemap_t;

uint32_t nextTargetBlock(const ReiserFs &fs, uint32_t previous) {
    uint32_t fs_size = fs.sizeInBlocks();
    uint32_t next = previous + 1;
    while ((next < fs_size) && fs.blockReserved(next)) { next ++; }
    if (next < fs_size) return next;
    else return 0; // no one found
}

void
createLargeScaleMovemap(const ReiserFs &fs, movemap_t &movemap, uint32_t sizelimit = 0)
{
    std::vector<ReiserFs::tree_element> *tree = fs.enumerateTree();
    std::vector<ReiserFs::tree_element>::const_iterator iter;

    if (0 == sizelimit)
        sizelimit = fs.sizeInBlocks();

    uint32_t free_idx = 0;

    // move all internal nodes to beginning of partition
    for (iter = tree->begin(); iter != tree->end(); ++ iter) {
        if (iter->type == BLOCKTYPE_INTERNAL) {
            free_idx = nextTargetBlock(fs, free_idx);
            assert (free_idx != 0);
            if (iter->idx != free_idx) { // do not add degenerate moves
                assert (movemap.count(iter->idx) == 0);
                movemap[iter->idx] = free_idx;
            }
        }
    }

    for (iter = tree->begin(); iter != tree->end() && movemap.size() < sizelimit; ++ iter) {
        if (iter->type == BLOCKTYPE_LEAF) {
            free_idx = nextTargetBlock(fs, free_idx);
            assert (free_idx != 0);
            if (iter->idx != free_idx) {
                assert (movemap.count(iter->idx) == 0);
                movemap[iter->idx] = free_idx;
            }
            Block *block_obj = fs.readBlock(iter->idx);
            for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
                const Block::item_header &ih = block_obj->itemHeader(k);
                if (ih.type() != KEY_TYPE_INDIRECT) continue;

                for (uint32_t idx = 0; idx < ih.length/4; idx ++) {
                    uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                    free_idx = nextTargetBlock(fs, free_idx);
                    assert (free_idx != 0);
                    if (child_idx != free_idx) {
                        assert (movemap.count(child_idx) == 0);
                        movemap[child_idx] = free_idx;
                    }
                }

                if (movemap.size() >= sizelimit) break;
            }
            fs.releaseBlock(block_obj);
        }
    }
    delete tree;
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
extractCleanMoves(const ReiserFs &fs, const movemap_t &movemap, movemap_t &clean_moves)
{
    clean_moves.clear();
    for (movemap_t::const_iterator it = movemap.begin(); it != movemap.end(); ++ it) {
        uint32_t from = it->first;
        uint32_t to = it->second;
        if (fs.blockUsed(from) && !fs.blockReserved(from) && !fs.blockUsed(to))
            clean_moves[from] = to;
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
        if (!fs.blockReserved(cur) && fs.blockUsed(cur)) {
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

void
simpleDefrag(ReiserFs &fs)
{
    uint32_t blocks_moved = 0;
    movemap_t movemap;
    do {
        std::cout << "-------------------------------------------------------------" << std::endl;
        movemap.clear();
        createLargeScaleMovemap(fs, movemap);
        std::cout << "movemap size = " << movemap.size() << std::endl;

        movemap_t clean_moves;
        extractCleanMoves(fs, movemap, clean_moves);
        std::cout << "clean_moves size = " << clean_moves.size() << std::endl;

        blocks_moved = fs.moveMultipleBlocks(clean_moves);
    } while (blocks_moved > 0);
}

void
simpleDefragWithPreclean(ReiserFs &fs, uint32_t sizelimit = 0)
{
    uint32_t blocks_moved = 0;
    do {
        std::cout << "-------------------------------------------------------------" << std::endl;
        // prepare list of movements
        movemap_t movemap;
        createLargeScaleMovemap(fs, movemap, sizelimit);
        std::cout << "movemap size = " << movemap.size() << std::endl;

        // remember targets of movements, so they will not be used in preclean stage
        std::set<uint32_t> occup;
        for (movemap_t::iterator it = movemap.begin(); it != movemap.end(); ++ it) {
            occup.insert(it->second);
        }

        // iterate over movements targets
        uint32_t free_idx = 0;
        movemap_t preclean;
        for (movemap_t::iterator it = movemap.begin(); it != movemap.end(); ++ it) {
            // only need those with occupied target blocks
            if (fs.blockUsed(it->second)) {
                do {
                    free_idx = fs.findFreeBlockAfter(free_idx);
                } while (occup.count(free_idx) > 0 && free_idx != 0);
                if (free_idx == 0) break; // failed to find free block
                preclean[it->second] = free_idx;
            }
        }

        // preclean stage: do actual move
        std::cout << "preclean size = " << preclean.size() << std::endl;
        fs.moveMultipleBlocks(preclean);

        // now extract clean moves, those that can be made without cleaning of targets
        movemap_t clean_moves;
        extractCleanMoves(fs, movemap, clean_moves);
        std::cout << "clean_moves size = " << clean_moves.size() << std::endl;
        blocks_moved = fs.moveMultipleBlocks(clean_moves);
    } while (blocks_moved > 0);
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
    // simpleDefrag(fs);
    simpleDefragWithPreclean(fs, 8000);


    fs.close();
    return 0;
}
