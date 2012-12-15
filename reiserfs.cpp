/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <signal.h>

void
assert_failfunc1(const std::string &expr, const std::string &filename, int lineno)
{
    std::stringstream ss;
    ss << "assertion failed: (" << expr << ") at " << filename << ":" << lineno;
    throw std::logic_error(ss.str());
}

void
assert_failfunc2(const std::string &msg, const std::string &expr, const std::string &filename,
                 int lineno)
{
    std::stringstream ss;
    ss << msg << " (" << expr << ") at " << filename << ":" << lineno;
    throw std::logic_error(ss.str());
}

void
assert_failfunc_s(const std::string &msg, const std::string &filename, int lineno)
{
    std::stringstream ss;
    ss << msg << " at " << filename << ":" << lineno;
    throw std::logic_error(ss.str());
}

ReiserFs::ReiserFs()
{
    this->closed = true;
    this->use_data_journaling = false;
    this->leaf_index_granularity = 2000;
    this->cache_size = 200;
}

ReiserFs::~ReiserFs()
{
    if (! this->closed) this->close();
}

void
ReiserFs::useDataJournaling(bool use)
{
    this->use_data_journaling = use;
}

int
ReiserFs::validateSuperblock()
{
    char buf_for_last_block[BLOCKSIZE];

    // check magic string
    if (0 != memcmp("ReIsEr2Fs", this->sb.s_magic, 10)) {
        std::cout << "error (sb): wrong superblock magic string" << std::endl;
        return RFSD_FAIL;
    }

    // check if last block can be read. readBuf
    try {
        readBufAt(this->fd, this->sb.s_block_count - 1, buf_for_last_block, BLOCKSIZE);
    } catch (std::logic_error &le) {
        std::cout << "error (sb): can't read last block of partition" << std::endl;
        return RFSD_FAIL;
    }

    // free block count can't be larger than total block count
    // TODO: take into account journal size, bitmap blocks, superblock and first 64k
    if (this->sb.s_free_blocks >= this->sb.s_block_count) {
        std::cout << "error (sb): too many free blocks in sb" << std::endl;
        return RFSD_FAIL;
    }

    // root block must reside somewhere inside partition
    if (this->sb.s_root_block >= this->sb.s_block_count) {
        std::cout << "error (sb): root block points outside partition" << std::endl;
        return RFSD_FAIL;
    }

    // journal position
    if (this->sb.jp_journal_1st_block + this->sb.jp_journal_size + 1 >= this->sb.s_block_count) {
        std::cout << "error (sb): journal doesn't fit into partition" << std::endl;
        return RFSD_FAIL;
    }

    // max transaction
    if (this->sb.jp_journal_trans_max + 2 > this->sb.jp_journal_size) {
        std::cout << "error (sb): max transaction size exceeds journal size" << std::endl;
        return RFSD_FAIL;
    }

    // check block size
    if (this->sb.s_blocksize != BLOCKSIZE) {
        std::cout << "error (sb): blocksize of " << this->sb.s_blocksize <<
            " not supported" << std::endl;
        return RFSD_FAIL;
    }

    // TODO: check oid cur and max sizes

    // umount state flag
    if (UMOUNT_STATE_CLEAN != this->sb.s_umount_state &&
        UMOUNT_STATE_DIRTY != this->sb.s_umount_state)
    {
        std::cout << "error (sb): umount state flag has wrong value" << std::endl;
        return RFSD_FAIL;
    }

    // TODO: check s_hash_function_code

    // tree height must be in [1,7]
    if (0 == this->sb.s_tree_height || this->sb.s_tree_height > 7) {
        std::cout << "error (sb): wrong tree height (" << this->sb.s_tree_height << ")" << std::endl;
        return RFSD_FAIL;
    }

    // check bitmap block count
    if (this->sb.s_bmap_nr != (this->sb.s_block_count-1)/BLOCKS_PER_BITMAP + 1) {
        std::cout << "error (sb): wrong bitmap block count" << std::endl;
        return RFSD_FAIL;
    }

    return RFSD_OK;
}

int
ReiserFs::open(const std::string &name, bool o_sync)
{
    this->fname = name;
    if (o_sync) {
        fd = ::open(name.c_str(), O_RDWR | O_SYNC | O_LARGEFILE);
    } else {
        fd = ::open(name.c_str(), O_RDWR | O_LARGEFILE);
    }

    if (-1 == fd) {
        std::cout << "error: can't open file `" << name << "', errno = " << errno << std::endl;
        return RFSD_FAIL;
    }

    if (RFSD_OK != this->readSuperblock()) {
        std::cout << "error: can't read superblock" << std::endl;
        return RFSD_FAIL;
    }

    if (RFSD_OK != this->validateSuperblock())
        return RFSD_FAIL;

    if (this->sb.s_umount_state != UMOUNT_STATE_CLEAN) {
        std::cout << "error: fs dirty, run fsck." << std::endl;
        return RFSD_FAIL;
    }
    this->journal = new FsJournal(this->fd, &this->sb);
    this->journal->setCacheSize(this->cache_size);
    this->bitmap = new FsBitmap(this->journal, &this->sb);
    this->closed = false;
    this->bitmap->setAGSize(AG_SIZE_128M);

    // initialize sealed AG list
    this->sealed_ags.clear();
    this->sealed_ags.resize(this->bitmap->AGCount(), false);

    // mark fs dirty
    this->sb.s_umount_state = UMOUNT_STATE_DIRTY;
    this->journal->beginTransaction();
    this->writeSuperblock();
    this->journal->commitTransaction();

    if (RFSD_FAIL == this->createLeafIndex())
        return RFSD_FAIL;

    return RFSD_OK;
}

int
ReiserFs::readSuperblock()
{
    int res = readBufAt(this->fd, SUPERBLOCK_BLOCK, &this->sb, sizeof(this->sb));
    return res;
}

void
ReiserFs::writeSuperblock()
{
    Block *sb_obj = this->journal->readBlock(SUPERBLOCK_BLOCK);
    ::memcpy (sb_obj->buf, &this->sb, sizeof(this->sb));
    sb_obj->markDirty(); // crucial, as without markDirty journal will skip block
    this->journal->releaseBlock(sb_obj);
}

void
ReiserFs::dumpSuperblock()
{
    std::cout << "dumpSuperblock() --------------------------------------" << std::endl;
    std::cout << "block count = " << sb.s_block_count << std::endl;
    std::cout << "free block count = " << sb.s_free_blocks << std::endl;
    std::cout << "root block at = " << sb.s_root_block << std::endl;
    std::cout << "journal start = " << sb.jp_journal_1st_block << std::endl;
    std::cout << "journal dev = " << sb.jp_journal_dev << std::endl;
    std::cout << "journal size = " << sb.jp_journal_size << std::endl;
    std::cout << "journal max transactions = " << sb.jp_journal_trans_max << std::endl;
    std::cout << "journal magic = " << sb.jp_journal_magic << std::endl;
    std::cout << "journal max batch = " << sb.jp_journal_max_batch <<std::endl;
    std::cout << "journal max commit age = " << sb.jp_journal_max_commit_age << std::endl;
    std::cout << "journal max transaction age = " << sb.jp_journal_max_trans_age << std::endl;
    std::cout << "block size = " << sb.s_blocksize << std::endl;
    std::cout << "max object id array size = " << sb.s_oid_maxsize <<std::endl;
    std::cout << "cur object id array size = " << sb.s_oid_cursize <<std::endl;
    std::cout << "unmount state = " << sb.s_umount_state << std::endl;
    std::cout << "magic = not implemeted" << std::endl;
    std::cout << "fsck state = " << sb.s_fs_state << std::endl;
    std::cout << "hash function = " << sb.s_hash_function_code << std::endl;
    std::cout << "tree height = " << sb.s_tree_height << std::endl;
    std::cout << "bitmap blocks count = " << sb.s_bmap_nr << std::endl;
    std::cout << "version = " << sb.s_version << std::endl;
    std::cout << "size of journal area = " << sb.s_reserved_for_journal << std::endl;
    std::cout << "inode generation = " << sb.s_inode_generation << std::endl;
    std::cout << "flags = " << sb.s_flags << std::endl;
    std::cout << "uuid = not implemented" << std::endl;
    std::cout << "label = not implemented" << std::endl;
    std::cout << "mount count = " << sb.s_mnt_count << std::endl;
    std::cout << "max mount count = " << sb.s_max_mnt_count << std::endl;
    std::cout << "last check = " << sb.s_lastcheck << std::endl;
    std::cout << "check interval = " << sb.s_check_interval << std::endl;
    std::cout << "unused fields dump = not implemented" << std::endl;
    std::cout << "=======================================================" << std::endl;
}

void
ReiserFs::dumpMovemap(const movemap_t &movemap) const
{
    std::cout << "movemap: ";
    for (movemap_t::const_iterator iter = movemap.begin(); iter != movemap.end(); ++ iter) {
        if (movemap.begin() != iter)
            std::cout << ", ";
        std::cout << iter->first << "->" << iter->second;
    }
    std::cout << std::endl;
}

void
ReiserFs::close()
{
    if (this->closed)   // don't do anything if fs already closed
        return;
    // clean fs dirty flag
    this->sb.s_umount_state = UMOUNT_STATE_CLEAN;
    this->journal->beginTransaction();
    this->writeSuperblock();
    this->journal->commitTransaction();

    // FsBitmap deletes its blocks itself, so if FsJournal desctructor will be called later
    // that FsBitmap's one, there can be case when block_cache have bitmap blocks, which
    // already freed by FsBitmap destructor. That may lead to read freed memory
    // So keep order: first journal, then bitmap.
    delete this->journal;
    delete this->bitmap;
    ::close(this->fd);
    this->closed = true;
}

void
ReiserFs::cleanupRegionMoveDataDown(uint32_t from, uint32_t to)
{
    std::vector<uint32_t> leaves;
    this->getLeavesForBlockRange(leaves, from, to);

    // move unformatted
    uint32_t free_idx = this->findFreeBlockAfter(to);
    assert1 (free_idx != 0);
    movemap_t movemap;
    std::set<Block::key_t> key_list;
    for (uint32_t k = 0; k < leaves.size(); k ++) {
        uint32_t leaf_idx = leaves[k];
        Block *block_obj = this->journal->readBlock(leaf_idx);
        block_obj->checkLeafNode();
        movemap.clear();
        key_list.clear();
        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            bool use_key = false;
            for (uint32_t idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih, idx);
                if (0 == child_idx)     // sparse file
                    continue;
                if (from <= child_idx && child_idx <= to) {
                    movemap[child_idx] = free_idx;
                    free_idx = this->findFreeBlockAfter(free_idx);
                    assert1 (free_idx != 0);
                    use_key = true;
                }
            }
            if (use_key) {
                key_list.insert(ih.key);
            }
        }
        this->journal->releaseBlock(block_obj);
        this->leafContentMoveUnformatted(leaf_idx, movemap, key_list);
        assert2 ("something left in movemap", movemap.size() == 0);
    }

    // now, when unformatted blocks moved, time to move tree nodes
    // create movemap
    movemap.clear();
    for (uint32_t c_idx = from; c_idx <= to; c_idx ++) {
        if (this->bitmap->blockReserved(c_idx)) continue;
        if (not this->bitmap->blockUsed(c_idx)) continue;
        movemap[c_idx] = free_idx;
        free_idx = this->findFreeBlockAfter(free_idx);
        assert1 (free_idx != 0);
    }
    this->moveBlocks(movemap); // will move only tree nodes, should be fast
    this->journal->flushTransactionCache();
    this->updateLeafIndex();
}

int
ReiserFs::createLeafIndex()
{
    uint32_t basket_count = (this->sizeInBlocks() - 1) / this->leaf_index_granularity + 1;
    this->leaf_index.clear();
    this->leaf_index.resize (basket_count);

    std::vector<tree_element> tree;
    recursivelyEnumerateNodes(this->sb.s_root_block, tree, false);

    Progress progress(tree.size());
    progress.setName("[leaf index]");

    for (std::vector<tree_element>::iterator it = tree.begin(); it != tree.end(); ++ it) {
        progress.inc();
        if (it->type != BLOCKTYPE_LEAF)
            continue;
        Block *block_obj = this->journal->readBlock(it->idx, false);
        block_obj->checkLeafNode();
        for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
            const struct Block::item_header &ih = block_obj->itemHeader(k);
            // indirect items contain links to unformatted (data) blocks
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (int idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih, idx);
                if (0 == child_idx)     // sparse file
                    continue;
                uint32_t basket_id = child_idx / this->leaf_index_granularity;
                this->leaf_index[basket_id].leaves.insert(it->idx);
            }
        }
        this->journal->releaseBlock(block_obj);
        if (ReiserFs::userAskedForTermination()) {
            progress.abort();
            return RFSD_FAIL;
        }
    }

    progress.show100();
    return RFSD_OK;
}

void
ReiserFs::updateLeafIndex()
{
    for (uint32_t basket_id = 0; basket_id < this->leaf_index.size(); basket_id ++) {
        leaf_index_entry &basket = this->leaf_index[basket_id];
        if (not basket.changed)
            continue;
        std::set<uint32_t>::iterator leaf_iter = basket.leaves.begin();
        while (leaf_iter != basket.leaves.end()) {
            uint32_t block_idx = *leaf_iter;
            Block *block_obj = this->journal->readBlock(block_idx, false);
            block_obj->checkLeafNode();
            bool leaf_has_link = false;
            for (uint32_t item_id = 0; item_id < block_obj->itemCount(); item_id ++) {
                const struct Block::item_header &ih = block_obj->itemHeader(item_id);
                if (KEY_TYPE_INDIRECT != ih.type())
                    continue;
                for (int idx = 0; idx < ih.length/4; idx ++) {
                    uint32_t target_idx = block_obj->indirectItemRef(ih, idx);
                    if (0 == target_idx)        // sparse file
                        continue;
                    uint32_t target_basket = target_idx / this->leaf_index_granularity;
                    if (target_basket == basket_id) {
                        leaf_has_link = true;
                        break;
                    }
                }
                if (leaf_has_link) break;
            }
            this->journal->releaseBlock(block_obj);
            if (not leaf_has_link) basket.leaves.erase(leaf_iter ++);
            else ++leaf_iter;
        }
        basket.changed = false;
    }
}

bool
ReiserFs::movemapConsistent(const movemap_t &movemap)
{
    movemap_t revmap;
    movemap_t::const_iterator mapiter;

    // last journal block is journal header block, its length is one
    const uint32_t journal_last_block =
        this->sb.jp_journal_1st_block + (this->sb.jp_journal_size - 1) + 1;

    for (mapiter = movemap.begin(); mapiter != movemap.end(); ++ mapiter) {
        // check if all 'from' blocks are occupied
        if (! this->bitmap->blockUsed(mapiter->first)) {
            err_string = "some 'from' block are not occupied";
            return false;
        }
        // check if all 'to' blocks are free
        if (this->bitmap->blockUsed(mapiter->second)) {
            err_string = "some 'to' block are not free";
            return false;
        }
        // check if any 'from' block are in reserved locations
        if (mapiter->first == SUPERBLOCK_BLOCK) {
            err_string = "some 'from' blocks map to superblock";
            return false;
        }
        // first 64 kiB are reserved
        if (mapiter->first < 65536/BLOCKSIZE) {
            err_string = "some 'from' blocks map to first 64 kiB";
            return false;
        }
        // bitmap blocks can't be moved
        if (mapiter->first == FIRST_BITMAP_BLOCK || mapiter->first % BLOCKS_PER_BITMAP == 0) {
            err_string = "some 'from' blocks map to bitmap blocks";
            return false;
        }
        if (this->sb.jp_journal_1st_block <= mapiter->first
                && mapiter->first <= journal_last_block)
        {
            err_string = "some 'from' blocks map to journal";
            return false;
        }
        // block_idx always greater than zero (unsigned int), no need to check that.
        // But upper bound should be checked.
        if (mapiter->first >= this->sb.s_block_count) {
            err_string = "some 'from' blocks map beyound filesystem limits";
            return false;
        }
        if (mapiter->second >= this->sb.s_block_count) {
            err_string = "some 'to' blocks map beyound filesystem limits";
            return false;
        }

        // build inverse transform
        revmap[mapiter->second] = mapiter->first;
    }
    // check for singularity
    if (revmap.size() != movemap.size()) {
        err_string = "movemap degenerate";
        return false;
    }
    return true;
}

uint32_t
ReiserFs::moveBlocks(movemap_t &movemap)
{
    if (0 == movemap.size())
        return 0;

    if (! this->movemapConsistent(movemap)) {
        std::cout << "error: movemap not consistent, " << this->err_string << std::endl;
        return 0;
    }

    this->blocks_moved_formatted = 0;
    this->blocks_moved_unformatted = 0;

    std::vector<uint32_t> leaves;
    std::set<Block::key_t> stub_empty_list;
    this->getLeavesForMovemap(leaves, movemap);

    for (std::vector<uint32_t>::const_iterator it = leaves.begin(); it != leaves.end(); ++ it) {
        uint32_t leaf_idx = *it;
        // move items with all keys from specific leaf
        this->leafContentMoveUnformatted(leaf_idx, movemap, stub_empty_list, true);
    }

    // tree nodes
    uint32_t tree_height = this->estimateTreeHeight();  // estimate tree height
    // move internal nodes, from layer 2 to sb.s_tree_height
    for (uint32_t t_level = TREE_LEVEL_LEAF + 1; t_level <= tree_height; t_level ++)
    {
        this->recursivelyMoveInternalNodes(this->sb.s_root_block, movemap, t_level);
    }

    // previous call moves all but root_block, move it if necessary
    if (movemap.count(this->sb.s_root_block)) {
        uint32_t old_root_block_idx = this->sb.s_root_block;
        this->journal->beginTransaction();
        // move root block itself
        this->journal->moveRawBlock(this->sb.s_root_block, movemap[this->sb.s_root_block]);
        // update bitmap
        this->bitmap->markBlockFree(this->sb.s_root_block);
        this->bitmap->markBlockUsed(movemap[this->sb.s_root_block]);
        // update s_root_block field in superblock and write it down through journal
        this->sb.s_root_block = movemap[this->sb.s_root_block];
        this->writeSuperblock();
        this->bitmap->writeChangedBitmapBlocks();
        this->journal->commitTransaction();
        movemap.erase(old_root_block_idx);
    }
    assert2 ("movemap should be empty after moveBlocks()", movemap.size() == 0);

    // make cached transaction to flush on disk
    this->journal->flushTransactionCache();
    // wipe obsolete entries out of leaf index
    this->updateLeafIndex();
    // rescan changed allocation groups
    this->bitmap->updateAGFreeExtents();

    return (this->blocks_moved_unformatted + this->blocks_moved_formatted);
}

Block*
ReiserFs::readBlock(uint32_t block) const
{
    assert1 (this->journal != NULL);
    return this->journal->readBlock(block);
}

void
ReiserFs::releaseBlock(Block *block) const
{
    assert1 (this->journal != NULL);
    journal->releaseBlock(block);
}

uint32_t
ReiserFs::estimateTreeHeight()
{
    Block *block_obj = this->journal->readBlock(this->sb.s_root_block);
    uint32_t root_block_level = block_obj->level();
    if (TREE_LEVEL_LEAF == root_block_level) block_obj->checkLeafNode();
    else block_obj->checkInternalNode();

    this->journal->releaseBlock(block_obj);
    return root_block_level;
}

/// \brief moves internal nodes and leaves
void
ReiserFs::recursivelyMoveInternalNodes(uint32_t block_idx, movemap_t &movemap,
    uint32_t target_level)
{
    /* we reach node on target_level (>=2), move nodes which it refers (as
    raw data),  and update  pointers in that node.  That form  a complete
    transaction. No internal node could have move than (4096-24-8)/(16+8)+1 =
    = 170 pointers, so transaction will have at most 170+1 blocks plus affected
    bitmap blocks. In worst case every block can change one bitmap, thus
    resulting in 171 bitmap blocks. So upper bound on transaction size is
    342 blocks, which is smaller than default 1024-block max transaction size.
    */
    Block *block_obj = this->journal->readBlock(block_idx);
    block_obj->checkInternalNode();
    uint32_t level = block_obj->level();

    if (level > target_level) {
        // if we are not on target_level, go deeper
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++ ) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyMoveInternalNodes(child_idx, movemap, target_level);
        }
        this->journal->releaseBlock(block_obj);
    } else {
        assert1 (level == target_level); // we reached target_level
        this->journal->beginTransaction();
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++ ) {
            uint32_t child_idx = block_obj->ptr(k).block;
            if (movemap.count(child_idx) > 0) {
                // move pointed block
                this->journal->moveRawBlock(child_idx, movemap[child_idx]);
                this->blocks_moved_formatted ++;
                // update bitmap
                this->bitmap->markBlockFree(child_idx);
                this->bitmap->markBlockUsed(movemap[child_idx]);
                // update pointer
                block_obj->ptr(k).block = movemap[child_idx];
                block_obj->markDirty();
                // if transaction becomes too large, divide it into smaller ones
                if (this->journal->estimateTransactionSize() > 100) {
                    if (block_obj->dirty)
                        this->journal->writeBlock(block_obj);
                    this->bitmap->writeChangedBitmapBlocks();
                    this->journal->commitTransaction();
                    this->journal->beginTransaction();
                }
                // update in-memory leaf index
                if (TREE_LEVEL_LEAF == level - 1) {
                    // child_idx pointing to leaf node, which moved just now
                    // so we must update all occurences of child_idx to movemap[child_idx]
                    // in leaf_index
                    for (std::vector<leaf_index_entry>::iterator it = this->leaf_index.begin();
                        it != this->leaf_index.end(); ++ it)
                    {
                        if (it->leaves.count(child_idx) > 0) {
                            it->leaves.erase(child_idx);
                            it->leaves.insert(movemap[child_idx]);
                        }
                    }
                }
                movemap.erase(child_idx);
            }
        }
        this->journal->releaseBlock(block_obj);
        this->bitmap->writeChangedBitmapBlocks();
        this->journal->commitTransaction();
    }
}

void
ReiserFs::recursivelyMoveUnformatted(uint32_t block_idx, movemap_t &movemap)
{
    Block *block_obj = this->journal->readBlock(block_idx);
    uint32_t level = block_obj->level();
    if (level > TREE_LEVEL_LEAF) {
        block_obj->checkInternalNode();
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyMoveUnformatted(child_idx, movemap);
        }
        this->journal->releaseBlock(block_obj);
    } else {
        // leaf level
        block_obj->checkLeafNode();
        this->journal->beginTransaction();
        for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
            const struct Block::item_header &ih = block_obj->itemHeader(k);
            // indirect items contain links to unformatted (data) blocks
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (int idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih, idx);
                if (0 == child_idx)     // sparse file
                    continue;
                if (movemap.count(child_idx) == 0) continue;
                // update pointers in indirect item
                block_obj->setIndirectItemRef(ih, idx, movemap[child_idx]);
                // actually move block
                bool should_journal_data = this->use_data_journaling;
                this->journal->moveRawBlock(child_idx, movemap[child_idx], should_journal_data);
                this->blocks_moved_unformatted ++;
                // update bitmap
                this->bitmap->markBlockFree(child_idx);
                this->bitmap->markBlockUsed(movemap[child_idx]);
                // if transaction becomes too large, divide it into smaller ones
                if (this->journal->estimateTransactionSize() > 100) {
                    if (block_obj->dirty)
                        this->journal->writeBlock(block_obj);
                    this->bitmap->writeChangedBitmapBlocks();
                    this->journal->commitTransaction();
                    this->journal->beginTransaction();
                }
                // update in-memory leaf index
                uint32_t new_basket_id = movemap[child_idx] / this->leaf_index_granularity;
                uint32_t old_basket_id = child_idx / this->leaf_index_granularity;
                this->leaf_index[new_basket_id].leaves.insert(block_idx);
                this->leaf_index[new_basket_id].changed = true;
                this->leaf_index[old_basket_id].changed = true;
            }
        }
        this->journal->releaseBlock(block_obj);
        this->bitmap->writeChangedBitmapBlocks();
        this->journal->commitTransaction();
    }
}

void
ReiserFs::leafContentMoveUnformatted(uint32_t block_idx, movemap_t &movemap,
                                     const std::set<Block::key_t> &key_list, bool all_keys)
{
    Block *block_obj = this->journal->readBlock(block_idx);
    block_obj->checkLeafNode();
    this->journal->beginTransaction();
    for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
        const struct Block::item_header &ih = block_obj->itemHeader(k);
        // indirect items contain links to unformatted (data) blocks
        if (KEY_TYPE_INDIRECT != ih.type())
            continue;
        if (not all_keys && (key_list.count(ih.key) == 0))
            continue;
        for (int idx = 0; idx < ih.length/4; idx ++) {
            uint32_t child_idx = block_obj->indirectItemRef(ih, idx);
            if (0 == child_idx)     // sparse file
                continue;
            if (movemap.count(child_idx) == 0) continue;
            uint32_t target_idx = movemap.find(child_idx)->second;
            // update pointers in indirect item
            block_obj->setIndirectItemRef(ih, idx, target_idx);
            // actually move block
            bool should_journal_data = this->use_data_journaling;
            this->journal->moveRawBlock(child_idx, target_idx, should_journal_data);
            this->blocks_moved_unformatted ++;
            // update bitmap
            this->bitmap->markBlockFree(child_idx);
            this->bitmap->markBlockUsed(target_idx);
            // if transaction becomes too large, divide it into smaller ones
            if (this->journal->estimateTransactionSize() > 100) {
                if (block_obj->dirty)
                    this->journal->writeBlock(block_obj);
                this->bitmap->writeChangedBitmapBlocks();
                this->journal->commitTransaction();
                this->journal->beginTransaction();
            }
            // update in-memory leaf index
            uint32_t new_basket_id = target_idx / this->leaf_index_granularity;
            uint32_t old_basket_id = child_idx / this->leaf_index_granularity;
            this->leaf_index[new_basket_id].leaves.insert(block_idx);
            this->leaf_index[new_basket_id].changed = true;
            this->leaf_index[old_basket_id].changed = true;
            movemap.erase(child_idx);
        }
    }
    this->journal->releaseBlock(block_obj);
    this->bitmap->writeChangedBitmapBlocks();
    this->journal->commitTransaction();
}

void
ReiserFs::collectLeafNodeIndices(uint32_t block_idx, std::vector<uint32_t> &lni)
{
    Block *block_obj = this->journal->readBlock(block_idx);
    block_obj->checkInternalNode();
    if (block_obj->level() == TREE_LEVEL_LEAF + 1) {
        // this is pre-leaves layer, collect pointers
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++)
            lni.push_back(block_obj->ptr(k).block);
    } else {
        // visit lower levels
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++)
            this->collectLeafNodeIndices(block_obj->ptr(k).block, lni);
    }
    this->journal->releaseBlock(block_obj);
}

void
ReiserFs::looseWalkTree()
{
    std::vector<uint32_t> leaf_nodes;
    this->collectLeafNodeIndices(this->sb.s_root_block, leaf_nodes);
    std::sort(leaf_nodes.begin(), leaf_nodes.end());
    for (std::vector<uint32_t>::const_iterator iter = leaf_nodes.begin();
        iter != leaf_nodes.end(); ++ iter)
    {
        Block *block_obj = this->journal->readBlock(*iter);
        block_obj->checkLeafNode();
        this->journal->releaseBlock(block_obj);
    }
}

void
ReiserFs::printFirstFreeBlock()
{
    for (uint32_t k = 0; k < this->sb.s_block_count; k ++) {
        if (! this->bitmap->blockUsed(k)) {
            std::cout << "free block: " << k << std::endl;
            return;
        }
    }
    std::cout << "no free block found" << std::endl;
}

uint32_t
ReiserFs::findFreeBlockAfter(uint32_t block_idx) const
{
    for (uint32_t k = block_idx + 1; k < this->sb.s_block_count; k ++)
        if (not this->bitmap->blockUsed(k) && not this->bitmap->blockReserved(k))
            return k;
    return 0;
}

uint32_t
ReiserFs::findFreeBlockBefore(uint32_t block_idx) const
{
    if (block_idx == 0)     // there is no free blocks before 0. There is no block at all
        return 0;
    // losing 0th block, but it's reserved anyway
    for (uint32_t k = block_idx - 1; k > 0; k --)
        if (not this->bitmap->blockUsed(k) && not this->bitmap->blockReserved(k))
            return k;
    return 0;
}

uint32_t
ReiserFs::freeBlockCount() const
{
    return this->sb.s_free_blocks;
}

void
ReiserFs::getLeavesForBlockRange(std::vector<uint32_t> &leaves, uint32_t from, uint32_t to)
{
    // make sure parameters fit range
    uint32_t fs_size = this->sizeInBlocks();
    from = std::max(0u, std::min(from, fs_size - 1));
    to = std::max(0u, std::min(to, fs_size - 1));

    leaves.clear();
    uint32_t basket_from = from / this->leaf_index_granularity;
    uint32_t basket_to = to / this->leaf_index_granularity;

    for (uint32_t basket_id = basket_from; basket_id <= basket_to; basket_id ++) {
        leaves.insert(leaves.end(), this->leaf_index[basket_id].leaves.begin(),
            this->leaf_index[basket_id].leaves.end());
    }
    std::sort(leaves.begin(), leaves.end());
    leaves.erase(std::unique(leaves.begin(), leaves.end()), leaves.end());
}

void
ReiserFs::getLeavesForMovemap(std::vector<uint32_t> &leaves, const movemap_t &movemap)
{
    leaves.clear();
    std::vector<uint32_t> basket_list;
    for (movemap_t::const_iterator it = movemap.begin(); it != movemap.end(); ++ it) {
        uint32_t basket_id = it->first / this->leaf_index_granularity;
        basket_list.push_back(basket_id);
    }
    // remove duplicates
    std::sort(basket_list.begin(), basket_list.end());
    basket_list.erase(
        std::unique(basket_list.begin(), basket_list.end()),
        basket_list.end());

    for (std::vector<uint32_t>::iterator it = basket_list.begin(); it != basket_list.end(); ++ it) {
        uint32_t basket_id = *it;
        leaves.insert(leaves.end(), this->leaf_index[basket_id].leaves.begin(),
            this->leaf_index[basket_id].leaves.end());
    }

    // remove duplicates
    std::sort(leaves.begin(), leaves.end());
    leaves.erase(std::unique(leaves.begin(), leaves.end()), leaves.end());
}

void
ReiserFs::setupInterruptSignalHandler()
{
    struct sigaction sa;
    memset(&sa, 0, sizeof (struct sigaction));
    sa.sa_handler = &ReiserFs::interruptSignalHandler;
    if (0 != sigaction(SIGINT, &sa, NULL)) {
        std::cout << "Warning: can't install SIGTERM handler. You are slighly discouraged to " \
            "interrupt defragmentation. Although no one can stop you." << std::endl;
    }
}

int ReiserFs::interrupt_state = 0;

void
ReiserFs::interruptSignalHandler(int arg)
{
    (void)arg;	// not used

    const char *msg1 = "\nInterrupting\n";
    const char *msg2 = "\nI heard you first time! I need some time to wrap things up.\n";
    const char *msg3 = "\nArgh!\n";

    // incresing this variable should be enough
    interrupt_state ++;

    // interact with user
    switch (interrupt_state) {
        case 1: write(STDOUT_FILENO, msg1, strlen(msg1)); break;
        case 2: write(STDOUT_FILENO, msg2, strlen(msg2)); break;
        default: write(STDOUT_FILENO, msg3, strlen(msg3)); break;
    }
}

bool
ReiserFs::userAskedForTermination()
{
    return ReiserFs::interrupt_state > 0;
}

bool
ReiserFs::AGSealed(uint32_t ag)
{
    assert1 (ag < this->bitmap->AGCount());
    return this->sealed_ags[ag];
}

void
ReiserFs::sealAG(uint32_t ag)
{
    assert1 (ag < this->bitmap->AGCount());
    this->sealed_ags[ag] = true;
}

int
ReiserFs::squeezeDataBlocksInAG(uint32_t ag)
{
    assert1 (ag < this->bitmap->AGCount());
    const uint32_t block_begin = this->bitmap->AGBegin(ag);
    const uint32_t block_end = this->bitmap->AGEnd(ag);
    uint32_t packed_ptr = block_begin;  // end of packed area
    uint32_t front_ptr = block_begin;   // frontier

    while (this->bitmap->blockReserved(front_ptr)) {
        front_ptr ++;
        packed_ptr ++;
    }

    // create desired move map
    movemap_t movemap;
    while (front_ptr <= block_end) {
        if (this->blockUsed(front_ptr)) {
            if (front_ptr != packed_ptr)
                movemap[front_ptr] = packed_ptr;
            do { packed_ptr++; } while (this->bitmap->blockReserved(packed_ptr));
        }
        do { front_ptr++; } while (this->bitmap->blockReserved(front_ptr));
    }

    if (0 == movemap.size())    // all blocks are on their position already
        return RFSD_OK;

    // move map is likely to be degenerate, with cycles. We need to find way untangle it.
    // first, count how many free block we need
    uint32_t free_block_count = 0;
    for (movemap_t::const_iterator iter = movemap.begin(); iter != movemap.end(); ++ iter) {
        if (this->blockUsed(iter->second))
            free_block_count ++;
    }

    // allocate free blocks in large chunks
    std::vector<uint32_t> free_blocks;
    uint32_t ext_size = 2048;
    while (free_block_count > 0) {
        uint32_t wag = ag + 1;
        std::vector<uint32_t> w_blocks;
        const uint32_t count = std::min(ext_size, free_block_count);
        while (RFSD_FAIL == this->bitmap->allocateFreeExtent(wag, count, w_blocks, ag)) {
            ext_size /= 2;
            if (0 == ext_size)
                return RFSD_FAIL;
        }
        free_block_count -= count;
        free_blocks.insert(free_blocks.end(), w_blocks.begin(), w_blocks.end());
    }

    // give up if we haven't managed to find enough free blocks
    if (free_block_count > 0)
        return RFSD_FAIL;

    // sort free block pointers
    std::sort (free_blocks.begin(), free_blocks.end());

    // fill movemap for second stage
    movemap_t movemap2;
    std::vector<uint32_t>::const_iterator free_ptr = free_blocks.begin();
    for (movemap_t::iterator iter = movemap.begin(); iter != movemap.end(); ++ iter) {
        if (this->blockUsed(iter->second)) {
            movemap2[*free_ptr] = iter->second;
            iter->second = *free_ptr;
            ++ free_ptr;
        }
    }
    assert1 (free_ptr == free_blocks.end());

    // do actual moves
    this->moveBlocks(movemap);
    this->moveBlocks(movemap2);
    // free extents in touched AGs are rescanned in ->moveBlocks

    return RFSD_OK;
}

int
ReiserFs::sweepOutAG(uint32_t ag)
{
    assert1 (ag < this->bitmap->AGCount());

    if (this->sealed_ags[ag])   // don't try to sweep if this AG sealed
        return RFSD_FAIL;

    uint32_t blocks_needed = this->bitmap->AGUsedBlockCount(ag);
    if (0 == blocks_needed) // no need to do anything
        return RFSD_OK;

    // allocate free blocks in other AGs
    std::vector<uint32_t> free_blocks;
    uint32_t segment_size = 4096;
    uint32_t temp_ag = ag;
    while (blocks_needed > 0) {
        uint32_t cnt = std::min(segment_size, blocks_needed);
        std::vector<uint32_t> w_blocks;
        while (RFSD_FAIL == this->bitmap->allocateFreeExtent(temp_ag, cnt, w_blocks, ag)) {
            segment_size /= 2;
            if (0 == segment_size)  // can't allocate 0 blocks. That means we failed to allocate
                return RFSD_FAIL;   // single block. There is no sense to continue.
            cnt = std::min(segment_size, blocks_needed);
        }
        blocks_needed -= cnt;
        free_blocks.insert(free_blocks.end(), w_blocks.begin(), w_blocks.end());
    }

    // sort free block pointers
    std::sort (free_blocks.begin(), free_blocks.end());

    // fill move map
    movemap_t movemap;
    uint32_t qqq = 0;
    std::vector<uint32_t>::const_iterator free_ptr = free_blocks.begin();
    for (uint32_t k = this->bitmap->AGBegin(ag); k <= this->bitmap->AGEnd(ag); k ++) {
        if (this->blockUsed(k) && !this->bitmap->blockReserved(k)) {
            movemap[k] = *free_ptr;
            ++ free_ptr;
            qqq ++;
        }
    }
    assert1 (free_ptr == free_blocks.end());

    // perform move
    this->moveBlocks(movemap);

    return RFSD_OK;
}

void
ReiserFs::recursivelyEnumerateNodes(uint32_t block_idx, std::vector<ReiserFs::tree_element> &tree,
                                    bool only_internal_nodes) const
{
    Block *block_obj = this->journal->readBlock(block_idx);
    uint32_t level = block_obj->level();
    block_obj->checkInternalNode();
    tree_element te;
    te.idx = block_idx;
    te.type = BLOCKTYPE_INTERNAL;
    tree.push_back(te);
    if (level == TREE_LEVEL_LEAF + 1) { // just above leaf level
        if (not only_internal_nodes) {
            for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
                te.idx = block_obj->ptr(k).block;;
                te.type = BLOCKTYPE_LEAF;
                tree.push_back(te);
            }
        }
    } else {
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyEnumerateNodes(child_idx, tree, only_internal_nodes);
        }
    }
    this->journal->releaseBlock(block_obj);
}

void
ReiserFs::enumerateTree(std::vector<tree_element> &tree) const
{
    tree.clear();
    // last parameter: false, not only internal nodes
    this->recursivelyEnumerateNodes(this->sb.s_root_block, tree, false);
}

void
ReiserFs::enumerateInternalNodes(std::vector<tree_element> &tree) const
{
    tree.clear();
    // last parameter: true, only internal nodes
    this->recursivelyEnumerateNodes(this->sb.s_root_block, tree, true);
}

void
ReiserFs::recursivelyEnumerateLeaves(uint32_t block_idx, const Block::key_t &start_key,
                                     int &soft_threshold, Block::key_t left, Block::key_t right,
                                     std::vector<uint32_t> &leaves, Block::key_t &last_key) const
{
    Block *block_obj = this->journal->readBlock(block_idx);
    uint32_t level = block_obj->level();
    if (level > TREE_LEVEL_LEAF) {
        // internal node
        block_obj->checkInternalNode();
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            const Block::key_t new_left = (k > 0) ? block_obj->key(k-1) : left;
            const Block::key_t new_right = (k < block_obj->keyCount()) ? block_obj->key(k) : right;
            if (new_right > start_key) {
                this->recursivelyEnumerateLeaves(block_obj->ptr(k).block, start_key, soft_threshold,
                                                 new_left, new_right, leaves, last_key);
                if (soft_threshold < 0)
                    break;
            }
        }
    } else {
        // leaf node
        block_obj->checkLeafNode();
        // every run will touch last leaf from previous scan. To prevent adding leaf twice
        // we check each item key and skip those less than start_key
        bool touch_leaf = false;
        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (ih.key <= start_key)    // skip items with inappropriate keys
                continue;
            touch_leaf = true;
            last_key = ih.key;  // and update last_key
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            if (ih.key > start_key) {
                soft_threshold -= ih.length / 4; // decrease by number of unformatted blocks
            }
        }
        if (touch_leaf) {
            leaves.push_back(block_idx);
            soft_threshold --; // count leaf block itself
        }
    }

    this->journal->releaseBlock(block_obj);
}

void
ReiserFs::enumerateLeaves(const Block::key_t &start_key, int soft_threshold,
                          std::vector<uint32_t> &leaves, Block::key_t &last_key) const
{
    last_key = start_key;
    leaves.clear();
    this->recursivelyEnumerateLeaves(this->sb.s_root_block, start_key, soft_threshold,
                                     Block::zero_key, Block::largest_key, leaves, last_key);
}

bool
ReiserFs::recursivelyGetBlocksOfObject(const uint32_t leaf_idx, const Block::key_t &start_key,
                                       const uint32_t object_type, const Block::key_t left,
                                       const Block::key_t right, uint32_t &start_offset,
                                       blocklist_t &blocks, Block::key_t &next_key,
                                       uint32_t &next_offset, uint32_t &limit) const
{
    // you may find asking yourself, why does `limit' compare to 1, not to 0. That's because
    // no one likes to get leaf block as the last block in a batch. Next call will add it anyway
    // and we'll end up moving that block twice. Moreover, that move will result in one-block hole.
    // So we compare `limit' to 1, not to 0.

    bool should_continue = true;
    Block *block_obj = this->journal->readBlock(leaf_idx);
    uint32_t level = block_obj->level();
    if (level > TREE_LEVEL_LEAF) {
        // internal node
        block_obj->checkInternalNode();
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            const Block::key_t new_left = (k > 0) ? block_obj->key(k-1) : left;
            const Block::key_t new_right = (k < block_obj->keyCount()) ? block_obj->key(k) : right;
            if (new_right > start_key) {
                should_continue = this->recursivelyGetBlocksOfObject(block_obj->ptr(k).block,
                                    start_key, object_type, new_left, new_right, start_offset,
                                    blocks, next_key, next_offset, limit);
                if (! start_key.sameObjectAs(next_key))
                    break;
                if (! should_continue)
                    break;
            }
        }
    } else {
        // leaf node
        block_obj->checkLeafNode();
        uint32_t indirect_idx = 0;   //< indirect item index. 1-based
        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (ih.key < start_key)     // skip items with inappropriate keys
                continue;
            if (limit <= 1) {
                // start_offset equal to zero means we finished previous indirect item
                // and should advance next_key pointer to next one. Otherwise next_key
                // should be kept the same
                if (0 == start_offset)
                    next_key = ih.key;
                should_continue = false;
                break;
            }
            next_key = ih.key;          // update next_key
            // exit if current item belongs to another object
            if (! start_key.sameObjectAs(next_key))
                break;
            if (KEY_TYPE_INDIRECT == ih.type() && KEY_TYPE_INDIRECT == object_type) {
                indirect_idx ++;
                if (1 == indirect_idx && 0 == start_offset && limit > 1
                    && (ih.key.offset(ih.version) != 1))
                {
                    blocks.push_back(leaf_idx);
                    limit --;
                    assert1((limit & 0x80000000) == 0); // catch negative
                }
                uint32_t end_pos = ih.length/4;
                if (start_offset + limit < end_pos)
                    end_pos = start_offset + limit;
                for (uint32_t idx = start_offset; idx < end_pos; idx ++) {
                    blocks.push_back(block_obj->indirectItemRef(ih, idx));
                    limit --;
                    assert1((limit & 0x80000000) == 0); // catch negative
                }
                start_offset = end_pos;
                if (end_pos == ih.length/4)
                    start_offset = 0;
                next_offset = start_offset;
            }
            if (KEY_TYPE_DIRECTORY == ih.type() && KEY_TYPE_DIRECTORY == object_type) {
                blocks.push_back(leaf_idx);
            }
        }
    }

    this->journal->releaseBlock(block_obj);
    return should_continue;
}

void
ReiserFs::getIndirectBlocksOfObject(const Block::key_t &start_key, uint32_t start_offset,
                                    Block::key_t &next_key, uint32_t &next_offset,
                                    blocklist_t &blocks, uint32_t limit) const
{
    next_key = start_key;
    next_offset = 0;
    blocks.clear();
    this->recursivelyGetBlocksOfObject(this->sb.s_root_block, start_key, KEY_TYPE_INDIRECT,
                                       Block::zero_key, Block::largest_key, start_offset, blocks,
                                       next_key, next_offset, limit);
}

Block::key_t
ReiserFs::findObject(const std::string &fname) const
{
    std::string fullpath(fname), tmps;
    size_t pos;
    Block::key_t cur_dir(KEY_V1, 1, 2, 0, 0);   // initialize cur_dir to root directory

    // remove leading slashes
    fullpath = fullpath.substr(fullpath.find_first_not_of('/'));

    while (std::string::npos != (pos = fullpath.find('/'))) {
        tmps = fullpath.substr(0, pos);
        cur_dir = this->findObjectAt(tmps, cur_dir);
        if (cur_dir.sameObjectAs(Block::zero_key))
            break;
        fullpath = fullpath.substr(pos + 1);
    }

    if (cur_dir.sameObjectAs(Block::zero_key))
        return Block::zero_key;

    return this->findObjectAt(fullpath, cur_dir);
}

Block::key_t
ReiserFs::findObjectAt(const std::string &fname, const Block::key_t &at) const
{
    Block::key_t dir_key(KEY_V1, at.dir_id, at.obj_id, 0, 0);
    const uint32_t fname_hash = this->getStringHashR5(fname);

    uint32_t start_offset = 0, next_offset = 0; // dummy
    uint32_t limit = 10; // should be greater than 1 to prevent early exit. Kind of dummy var too.
    Block::key_t next_key;
    blocklist_t dir_leaves;
    this->recursivelyGetBlocksOfObject(this->sb.s_root_block, dir_key, KEY_TYPE_DIRECTORY,
                                       Block::zero_key, Block::largest_key, start_offset,
                                       dir_leaves, next_key, next_offset, limit);

    for (blocklist_t::iterator it = dir_leaves.begin(); it != dir_leaves.end(); ++ it) {
        const uint32_t leaf_idx = *it;
        Block *block_obj = this->journal->readBlock(leaf_idx);

        for (uint32_t item_idx = 0; item_idx < block_obj->itemCount(); item_idx ++) {
            const Block::item_header &ih = block_obj->itemHeader(item_idx);
            if (!ih.key.sameObjectAs(dir_key)) continue;
            if (KEY_TYPE_DIRECTORY == ih.type()) {
                for (uint32_t k = 0; k < ih.count; k ++) {
                    const struct Block::de_header &deh = block_obj->dirHeader(ih, k);
                    if (fname_hash == (deh.hash_gen & 0x7fffff80)
                        && fname == block_obj->dirEntryName(ih, k))
                    {
                        this->journal->releaseBlock(block_obj);
                        return Block::key_t(KEY_V1, deh.dir_id, deh.obj_id, 0, 0);
                    }
                }
            }
        }
        this->journal->releaseBlock(block_obj);
    }

    return Block::key_t(KEY_V1, 0, 0, 0, 0);
}

uint32_t
ReiserFs::getStringHashR5(const std::string &s) const
{
    uint32_t hash = 0;
    for (std::string::const_iterator it = s.begin(); it != s.end(); ++ it) {
        signed char c = *it;
        hash = (hash + (c << 4) + (c >> 4)) * 11;
    }
    return hash & 0x7fffff80;
}
