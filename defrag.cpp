#include "reiserfs.hpp"
#include <assert.h>
#include <iostream>
#include <vector>
#include <set>

typedef std::map<uint32_t, uint32_t> movemap_t;

class Defrag {
public:
    Defrag (ReiserFs &fs);
    void setSizeLimit(uint32_t size_limit);
    void simpleDefrag();
    void simpleDefragWithPreclean();

private:
    ReiserFs &fs;
    movemap_t movemap;
    movemap_t clean_moves;
    uint32_t size_limit;

    uint32_t nextTargetBlock(uint32_t previous);
    void createLargeScaleMovemap();
    uint32_t removeDegenerateEntries();
    void extractCleanMoves();

};

Defrag::Defrag(ReiserFs &fs) : fs(fs)
{
    this->size_limit = fs.sizeInBlocks();
}

void
Defrag::setSizeLimit(uint32_t size_limit)
{
    this->size_limit = size_limit;
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

void
Defrag::createLargeScaleMovemap()
{
    std::vector<ReiserFs::tree_element> *tree = this->fs.enumerateTree();
    std::vector<ReiserFs::tree_element>::const_iterator iter;

    uint32_t free_idx = 0;
    this->movemap.clear();

    // move all internal nodes to beginning of partition
    for (iter = tree->begin(); iter != tree->end(); ++ iter) {
        if (iter->type == BLOCKTYPE_INTERNAL) {
            free_idx = this->nextTargetBlock(free_idx);
            assert (free_idx != 0);
            if (iter->idx != free_idx) { // do not add degenerate moves
                assert (this->movemap.count(iter->idx) == 0);
                movemap[iter->idx] = free_idx;
            }
        }
    }

    for (iter = tree->begin(); iter != tree->end() && movemap.size() < this->size_limit; ++ iter) {
        if (iter->type == BLOCKTYPE_LEAF) {
            free_idx = this->nextTargetBlock(free_idx);
            assert (free_idx != 0);
            if (iter->idx != free_idx) {
                assert (this->movemap.count(iter->idx) == 0);
                this->movemap[iter->idx] = free_idx;
            }
            Block *block_obj = this->fs.readBlock(iter->idx);
            for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
                const Block::item_header &ih = block_obj->itemHeader(k);
                if (ih.type() != KEY_TYPE_INDIRECT) continue;

                for (uint32_t idx = 0; idx < ih.length/4; idx ++) {
                    uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                    free_idx = this->nextTargetBlock(free_idx);
                    assert (free_idx != 0);
                    if (child_idx != free_idx) {
                        assert (this->movemap.count(child_idx) == 0);
                        this->movemap[child_idx] = free_idx;
                    }
                }

                if (this->movemap.size() >= this->size_limit) break;
            }
            this->fs.releaseBlock(block_obj);
        }
    }
    delete tree;
}

uint32_t
Defrag::removeDegenerateEntries()
{
    movemap_t::iterator iter;
    movemap_t::iterator del_iter;
    uint32_t degenerate_count = 0;

    iter = this->movemap.begin();
    while (iter != movemap.end()) {
        del_iter = iter ++;
        if (del_iter->first == del_iter->second) {
            this->movemap.erase(del_iter);
            degenerate_count ++;
        }
    }
    return degenerate_count;
}

void
Defrag::extractCleanMoves()
{
    this->clean_moves.clear();
    for (movemap_t::const_iterator it = this->movemap.begin(); it != this->movemap.end(); ++ it) {
        uint32_t from = it->first;
        uint32_t to = it->second;
        if (this->fs.blockUsed(from) && !this->fs.blockReserved(from) && !this->fs.blockUsed(to))
            this->clean_moves[from] = to;
    }
}

void
Defrag::simpleDefrag()
{
    uint32_t blocks_moved = 0;
    do {
        std::cout << "-------------------------------------------------------------" << std::endl;
        this->movemap.clear();
        this->createLargeScaleMovemap();
        std::cout << "movemap size = " << this->movemap.size() << std::endl;

        this->extractCleanMoves();
        std::cout << "clean_moves size = " << this->clean_moves.size() << std::endl;

        blocks_moved = this->fs.moveMultipleBlocks(this->clean_moves);
    } while (blocks_moved > 0);
}

void
Defrag::simpleDefragWithPreclean()
{
    uint32_t blocks_moved = 0;
    do {
        std::cout << "-------------------------------------------------------------" << std::endl;
        // prepare list of movements
        this->createLargeScaleMovemap();
        std::cout << "movemap size = " << this->movemap.size() << std::endl;

        // remember targets of movements, so they will not be used in preclean stage
        std::set<uint32_t> occup;
        for (movemap_t::iterator it = this->movemap.begin(); it != this->movemap.end(); ++ it) {
            occup.insert(it->second);
        }

        // iterate over movements targets
        uint32_t free_idx = 0;
        movemap_t preclean;
        for (movemap_t::iterator it = this->movemap.begin(); it != this->movemap.end(); ++ it) {
            // only need those with occupied target blocks
            if (this->fs.blockUsed(it->second)) {
                do {
                    free_idx = this->fs.findFreeBlockAfter(free_idx);
                } while (occup.count(free_idx) > 0 && free_idx != 0);
                if (free_idx == 0) break; // failed to find free block
                preclean[it->second] = free_idx;
            }
        }

        // preclean stage: do actual move
        std::cout << "preclean size = " << preclean.size() << std::endl;
        this->fs.moveMultipleBlocks(preclean);

        // now extract clean moves, those that can be made without cleaning of targets
        this->extractCleanMoves();
        std::cout << "clean_moves size = " << this->clean_moves.size() << std::endl;
        blocks_moved = this->fs.moveMultipleBlocks(this->clean_moves);
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

    Defrag defrag(fs);
    defrag.simpleDefragWithPreclean();

    fs.close();
    return 0;
}
