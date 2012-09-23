#include "reiserfs.hpp"
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

ReiserFs::ReiserFs()
{
    this->closed = true;
    this->use_data_journaling = false;
    this->leaf_index_granularity = 2000;
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
ReiserFs::open(const std::string &name, bool o_sync)
{
    std::cout << "open " << name << std::endl;
    this->fname = name;
    if (o_sync) {
        fd = ::open(name.c_str(), O_RDWR | O_SYNC | O_LARGEFILE);
    } else {
        fd = ::open(name.c_str(), O_RDWR | O_LARGEFILE);
    }

    if (-1 == fd) {
        std::cerr << "error: can't open file `" << name << "', errno = " << errno << std::endl;
        return RFSD_FAIL;
    }

    this->readSuperblock();
    if (this->sb.s_umount_state != UMOUNT_STATE_CLEAN) {
        std::cerr << "error: fs dirty, run fsck." << std::endl;
        return RFSD_FAIL;
    }
    this->journal = new FsJournal(this->fd, &this->sb);
    this->bitmap = new FsBitmap(this->journal, &this->sb);
    this->closed = false;

    // mark fs dirty
    this->sb.s_umount_state = UMOUNT_STATE_DIRTY;
    this->journal->beginTransaction();
    this->writeSuperblock();
    this->journal->commitTransaction();

    this->createLeafIndex();

    return RFSD_OK;
}

void
ReiserFs::readSuperblock()
{
    readBufAt(this->fd, SUPERBLOCK_BLOCK, &this->sb, sizeof(this->sb));
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
ReiserFs::close()
{
    // clean fs dirty flag
    this->sb.s_umount_state = UMOUNT_STATE_CLEAN;
    this->journal->beginTransaction();
    this->writeSuperblock();
    this->journal->commitTransaction();

    std::cout << "ReiserFs::close, " << this->fname << std::endl;

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
ReiserFs::createLeafIndex()
{
    uint32_t basket_count = (this->sizeInBlocks() - 1) / this->leaf_index_granularity + 1;
    this->leaf_index.clear();
    this->leaf_index.resize (basket_count);

    std::vector<tree_element> tree;
    recursivelyEnumerateNodes(this->sb.s_root_block, tree);

    for (std::vector<tree_element>::iterator it = tree.begin(); it != tree.end(); ++ it) {
        if (it->type != BLOCKTYPE_LEAF)
            continue;
        Block *block_obj = this->journal->readBlock(it->idx, false);
        for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
            const struct Block::item_header &ih = block_obj->itemHeader(k);
            // indirect items contain links to unformatted (data) blocks
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (int idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                uint32_t basket_id = child_idx / this->leaf_index_granularity;
                this->leaf_index[basket_id].leaves.insert(it->idx);
            }
        }
        this->journal->releaseBlock(block_obj);
    }

    return;
}

void
ReiserFs::updateLeafIndex()
{
    for (uint32_t basket_id = 0; basket_id < this->leaf_index.size(); basket_id ++) {
        leaf_index_entry &basket = this->leaf_index[basket_id];
        if (not basket.changed)
            continue;
        std::set<uint32_t>::iterator leaf_iter;
        for (leaf_iter = basket.leaves.begin(); leaf_iter != basket.leaves.end(); ++ leaf_iter) {
            uint32_t block_idx = *leaf_iter;
            Block *block_obj = this->journal->readBlock(block_idx, false);
            bool leaf_has_link = false;
            for (uint32_t item_id = 0; item_id < block_obj->itemCount(); item_id ++) {
                const struct Block::item_header &ih = block_obj->itemHeader(item_id);
                if (KEY_TYPE_INDIRECT != ih.type())
                    continue;
                for (int idx = 0; idx < ih.length/4; idx ++) {
                    uint32_t target_idx = block_obj->indirectItemRef(ih.offset, idx);
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

void
ReiserFs::moveBlock(uint32_t from, uint32_t to)
{
    movemap_t movemap;
    movemap[from] = to;
    this->moveMultipleBlocks (movemap);
}

bool
ReiserFs::movemap_consistent(const movemap_t &movemap)
{
    movemap_t revmap;
    movemap_t::const_iterator mapiter;

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
        if (mapiter->first == FIRST_BITMAP_BLOCK
            || (mapiter->first/BLOCKS_PER_BITMAP)*BLOCKS_PER_BITMAP == mapiter->first)
        {
            err_string = "some 'from' blocks map to bitmap blocks";
            return false;
        }
        // first 64 kiB are reserved
        if (mapiter->first < 65536/BLOCKSIZE) {
            err_string = "some 'from' blocks map to first 64 kiB";
            return false;
        }
        // last journal block is journal header block, its length is no
        uint32_t journal_last_block = this->sb.jp_journal_1st_block
                                      + (this->sb.jp_journal_size - 1) + 1;
        if (mapiter->first >= this->sb.jp_journal_1st_block
            && mapiter->first <= journal_last_block)
        {
            err_string = "some 'from' blocks map to journal";
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

/// moves multiple blocks
///
/// \return number of blocks moved
uint32_t
ReiserFs::moveMultipleBlocks(movemap_t &movemap, const struct Block::key &key)
{
    if (! this->movemap_consistent(movemap)) {
        std::cerr << "error: movemap not consistent, " << this->err_string << std::endl;
        return 0;
    }

    uint32_t tree_height = this->estimateTreeHeight();
    // reset statistics
    this->blocks_moved_formatted = 0;
    this->blocks_moved_unformatted = 0;

    // first, move unformatted blocks
    this->recursivelyMoveUnformatted(this->sb.s_root_block, movemap, key);
    // then move internal nodes, from layer 2 to sb.s_tree_height
    for (uint32_t t_level = TREE_LEVEL_LEAF + 1; t_level <= tree_height; t_level ++)
    {
        this->recursivelyMoveInternalNodes(this->sb.s_root_block, movemap, t_level);
    }

    // previous call moves all but root_block, move it if necessary
    if (movemap.count(this->sb.s_root_block)) {
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
    }

    // make cached transaction to flush on disk
    this->journal->flushTransactionCache();
    // wipe obsolete entries out of leaf index
    this->updateLeafIndex();

    return (this->blocks_moved_unformatted + this->blocks_moved_formatted);
}


Block*
ReiserFs::readBlock(uint32_t block) const
{
    assert(this->journal != NULL);
    return this->journal->readBlock(block);
}

void
ReiserFs::releaseBlock(Block *block) const
{
    assert(this->journal != NULL);
    journal->releaseBlock(block);
}

uint32_t
ReiserFs::estimateTreeHeight()
{
    Block *block_obj = this->journal->readBlock(this->sb.s_root_block);
    uint32_t root_block_level = block_obj->level();
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
    uint32_t level = block_obj->level();

    if (level > target_level) {
        // if we are not on target_level, go deeper
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++ ) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyMoveInternalNodes(child_idx, movemap, target_level);
        }
        this->journal->releaseBlock(block_obj);
    } else {
        assert (level == target_level); // we reached target_level
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
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyMoveUnformatted(child_idx, movemap);
        }
        this->journal->releaseBlock(block_obj);
    } else {
        // leaf level
        this->journal->beginTransaction();
        for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
            const struct Block::item_header &ih = block_obj->itemHeader(k);
            // indirect items contain links to unformatted (data) blocks
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (int idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                if (movemap.count(child_idx) == 0) continue;
                // update pointers in indirect item
                block_obj->setIndirectItemRef(ih.offset, idx, movemap[child_idx]);
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
ReiserFs::recursivelyMoveUnformatted(uint32_t block_idx, movemap_t &movemap,
                                        const struct Block::key &key)
{
    Block *block_obj = this->journal->readBlock(block_idx);
    uint32_t level = block_obj->level();
    if (level > TREE_LEVEL_LEAF) {
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            uint32_t child_idx = block_obj->ptr(k).block;
            bool go_deeper = true;
            if (k > 0) go_deeper = go_deeper && (block_obj->key(k-1) <= key);
            if (k < block_obj->keyCount())
                go_deeper = go_deeper && (key <= block_obj->key(k));
            if (go_deeper)
                this->recursivelyMoveUnformatted(child_idx, movemap, key);
        }
        this->journal->releaseBlock(block_obj);
    } else {
        // leaf level
        this->journal->beginTransaction();
        for (uint32_t k = 0; k < block_obj->itemCount(); k ++) {
            const struct Block::item_header &ih = block_obj->itemHeader(k);
            // indirect items contain links to unformatted (data) blocks
            if (KEY_TYPE_INDIRECT != ih.type())
                continue;
            for (int idx = 0; idx < ih.length/4; idx ++) {
                uint32_t child_idx = block_obj->indirectItemRef(ih.offset, idx);
                if (movemap.count(child_idx) == 0) continue;
                // update pointers in indirect item
                block_obj->setIndirectItemRef(ih.offset, idx, movemap[child_idx]);
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
ReiserFs::collectLeafNodeIndices(uint32_t block_idx, std::vector<uint32_t> &lni)
{
    Block *block_obj = this->journal->readBlock(block_idx);
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
        if (not this->bitmap->blockUsed(k) && not this->blockReserved(k))
            return k;
    return 0;
}

uint32_t
ReiserFs::findFreeBlockBefore(uint32_t block_idx) const
{
    // lost 0th block, but it's reserved anyway
    for (uint32_t k = block_idx - 1; k > 0; k --)
        if (not this->bitmap->blockUsed(k) && not this->blockReserved(k))
            return k;
    return 0;
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

bool
ReiserFs::blockIsBitmap(uint32_t block_idx) const
{
    if (block_idx == FIRST_BITMAP_BLOCK)
        return true;
    if ((block_idx/BLOCKS_PER_BITMAP)*BLOCKS_PER_BITMAP == block_idx)
        return true;
    return false;
}

bool
ReiserFs::blockIsJournal(uint32_t block_idx) const
{
    uint32_t journal_start = this->sb.jp_journal_1st_block;
    // journal has one additional block for its 'header'
    uint32_t journal_end = journal_start + (this->sb.jp_journal_size - 1) + 1;
    return (journal_start <= block_idx) && (block_idx <= journal_end);
}

bool
ReiserFs::blockIsFirst64k(uint32_t block_idx) const
{
    return block_idx < 65536/BLOCKSIZE;
}

bool
ReiserFs::blockIsSuperblock(uint32_t block_idx) const
{
    return block_idx == SUPERBLOCK_BLOCK;
}

bool
ReiserFs::blockReserved(uint32_t block_idx) const
{
    return blockIsBitmap(block_idx) || blockIsJournal(block_idx) || blockIsFirst64k(block_idx)
        || blockIsSuperblock(block_idx);
}

void
ReiserFs::recursivelyEnumerateNodes(uint32_t block_idx, std::vector<ReiserFs::tree_element> &tree) const
{
    Block *block_obj = this->journal->readBlock(block_idx);
    uint32_t level = block_obj->level();
    ReiserFs::tree_element te;
    te.idx = block_idx;
    if (level > TREE_LEVEL_LEAF) {
        te.type = BLOCKTYPE_INTERNAL;
        tree.push_back(te);
        for (uint32_t k = 0; k < block_obj->ptrCount(); k ++) {
            uint32_t child_idx = block_obj->ptr(k).block;
            this->recursivelyEnumerateNodes(child_idx, tree);
        }
    } else {
        // leaf level
        te.type = BLOCKTYPE_LEAF;
        tree.push_back(te);
    }
    this->journal->releaseBlock(block_obj);
}

std::vector<ReiserFs::tree_element> *
ReiserFs::enumerateTree() const
{
    std::vector<ReiserFs::tree_element> *tree = new std::vector<ReiserFs::tree_element>;

    this->recursivelyEnumerateNodes(this->sb.s_root_block, *tree);
    return tree;
}
