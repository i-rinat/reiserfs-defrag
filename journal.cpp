/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <algorithm>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdlib>

FsJournal::FsJournal(int fd_, FsSuperblock *sb)
{
    this->fd = fd_;
    this->cache_hits = 0;
    this->cache_misses = 0;
    this->max_cache_size = 51200;
    this->transaction.running = false;
    this->use_journaling = true;
    this->sb = sb;
    this->flag_transaction_max_size_exceeded = false;

    // read journal header
    int res = readBufAt (this->fd, this->sb->jp_journal_1st_block + this->sb->jp_journal_size,
                                &journal_header, sizeof(journal_header));
    if (RFSD_OK != res) {
        assert2 ("can't read journal header", false);
    }

    // determine max transaction batch size
    this->max_batch_size = this->sb->jp_journal_max_batch;
    if (this->max_batch_size > 900) this->max_batch_size = 900;
}

FsJournal::~FsJournal()
{
    this->flushTransactionCache();
    // clear cache and check that all block left it
    std::map<uint32_t, cache_entry>::iterator it, dit;
    it = this->block_cache.begin();
    while (it != this->block_cache.end()) {
        dit = it ++;
        this->deleteFromCache(dit->first);
    }
    if (this->block_cache.size() > 0) {
        assert2 ("block cache still contains elements on FsJournal destruction", false);
    }

    std::cout << "blockcache statistics: " << this->cache_hits << "/" << this->cache_misses;
    std::cout << " (hits/misses)" << std::endl;
}

void
FsJournal::beginTransaction()
{
    if (not this->use_journaling) return;

    if (this->transaction.running) {
        assert2 ("nested transaction", false);
    }

    if (this->transaction.blocks.size() != 0 && not this->transaction.batch_running) {
        assert2 ("writes outside of transaction", false);
    }

    this->transaction.running = true;
    this->transaction.batch_running = true;
}

int
writeBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size)
{
#ifdef RFSD_DEBUG_READWRITE_TIMESTAMPS
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    fprintf (stderr, "%d.%09d ? %d\n", (int)ts.tv_sec, (int)ts.tv_nsec, block_idx);
#endif

    off_t ofs = static_cast<off_t>(block_idx) * BLOCKSIZE;
    off_t new_ofs = ::lseek (fd, ofs, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs || new_ofs != ofs) {
        assert2 ("seek failed (write)", false);
    }
    ssize_t bytes_written = ::write (fd, buf, size);
    if (-1 == bytes_written || static_cast<ssize_t>(size) != bytes_written) {
        assert2 ("write failed", false);
    }
    return RFSD_OK;
}

int
readBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size)
{
#ifdef RFSD_DEBUG_READWRITE_TIMESTAMPS
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    fprintf (stderr, "%d.%09d %d ?\n", (int)ts.tv_sec, (int)ts.tv_nsec, block_idx);
#endif

    off_t ofs = static_cast<off_t>(block_idx) * BLOCKSIZE;
    off_t new_ofs = ::lseek (fd, ofs, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs || new_ofs != ofs) {
        assert2 ("seek failed (write)", false);
    }
    ssize_t bytes_read = ::read (fd, buf, size);
    if (-1 == bytes_read || static_cast<ssize_t>(size) != bytes_read) {
        assert2 ("read failed", false);
    }
    return RFSD_OK;
}

int
FsJournal::writeJournalEntry()
{
    struct {
        uint32_t transaction_id;
        uint32_t length;
        uint32_t mount_id;
        uint32_t real_blocks[(BLOCKSIZE - 24)/4];
        uint8_t  magic[12];
    } description_block;

    struct {
        uint32_t transaction_id;
        uint32_t length;
        uint32_t real_blocks[(BLOCKSIZE - 24)/4];
        uint8_t  digest[16];
    } commit_block;

    memset (&description_block, 0, sizeof(description_block));
    memset (&commit_block, 0, sizeof(commit_block));

    uint32_t transaction_id = this->journal_header.last_flush_id + 1;
    uint32_t transaction_offset = this->journal_header.unflushed_offset;

    // check for proper alignment, just to be sure
    assert1 (sizeof(commit_block) == BLOCKSIZE);
    assert1 (sizeof(description_block) == BLOCKSIZE);

    // ensure transaction fits structures
    assert1 (this->transaction.blocks.size() + 2 <= 2*(BLOCKSIZE-24)/4);

    // fill description and commit blocks
    description_block.transaction_id = transaction_id;
    description_block.length = this->transaction.blocks.size();
    description_block.mount_id = journal_header.mount_id;
    memcpy (description_block.magic, "ReIsErLB", 8);
    commit_block.transaction_id = transaction_id;
    commit_block.length = this->transaction.blocks.size();

    uint32_t first_half = static_cast<uint32_t>((BLOCKSIZE-24)/4);
    uint32_t k = 0;
    for (std::set<Block *>::const_iterator iter = this->transaction.blocks.begin();
        iter != this->transaction.blocks.end(); ++ iter)
    {
        uint32_t block_idx = (*iter)->block;
        if (k < first_half) {
            description_block.real_blocks[k] = block_idx;
        } else if (k < 2*first_half) {
            commit_block.real_blocks[k - first_half] = block_idx;
        } else {
            assert2 ("too many blocks in transaction", false);
        }
        k ++;
    }

    uint32_t j_pos = transaction_offset; // cursor
    const uint32_t j_1st_block = this->sb->jp_journal_1st_block;

    // write desc block
    if (RFSD_OK != writeBufAt (this->fd, j_1st_block + j_pos, &description_block, BLOCKSIZE) )
        return RFSD_FAIL;
    j_pos = (j_pos + 1) % this->sb->jp_journal_size;

    // write data blocks
    for (std::set<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        if (RFSD_OK != writeBufAt (this->fd, j_1st_block + j_pos, (*it)->buf, BLOCKSIZE) )
            return RFSD_FAIL;
        j_pos = (j_pos + 1) % this->sb->jp_journal_size;
    }

    // write commit block
    if (RFSD_OK != writeBufAt (this->fd, j_1st_block + j_pos, &commit_block, BLOCKSIZE) )
        return RFSD_FAIL;

    return RFSD_OK;
}

int
FsJournal::doCommitTransaction()
{
    // transaction.blocks already sorted and deduplicated
    // write journal entry
    if (RFSD_OK != this->writeJournalEntry())
        return RFSD_FAIL;

    // update journal header, advance by number of blocks plus desc and commit blocks
    journal_header.unflushed_offset += 2 + this->transaction.blocks.size();
    journal_header.unflushed_offset %= this->sb->jp_journal_size; // wrap
    journal_header.last_flush_id ++;

    // ensure journal entry written
    if (0 != ::fdatasync(this->fd))
        return RFSD_FAIL;

    // write data to disk
    for (std::set<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        if (RFSD_OK != writeBufAt(this->fd, (*it)->block, (*it)->buf, BLOCKSIZE))
            return RFSD_FAIL;
    }

    // finally release blocks. Block can survive this if it has more than one reference,
    // like cached blocks. ->releaseBlock will not call writeBlock as block is not
    // dirty.
    for (std::set<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        uint32_t block_idx = (*it)->block;
        // reset block priority to normal. Contents written to disk, so cache entry
        // may safelly be deleted if needed
        if (this->block_cache.count(block_idx) > 0)
            this->block_cache[block_idx].priority = CACHE_PRIORITY_NORMAL;
        this->releaseBlock(*it);
    }
    this->transaction.blocks.clear();

    // ensure actual data written to the disk
    if (0 != ::fdatasync(this->fd))
        return RFSD_FAIL;

    // update journal header, thus closing transaction
    int res = writeBufAt (this->fd, this->sb->jp_journal_1st_block + this->sb->jp_journal_size,
        &journal_header, sizeof(journal_header));
    if (RFSD_OK != res)
        return RFSD_FAIL;

    this->transaction.running = false;
    return RFSD_OK;
}


int
FsJournal::commitTransaction()
{
    if (not this->use_journaling) return RFSD_OK;

    if (this->transaction.blocks.size() == 0) {
        // std::cout << "warning: empty transaction" << std::endl;
        this->transaction.running = false;
        return RFSD_OK;
    }

    if (this->transaction.blocks.size() > this->max_batch_size) {
        if (this->transaction.blocks.size() > this->sb->jp_journal_trans_max) {
            std::cout << "warning: transaction max size exceeded" << std::endl;
            this->flag_transaction_max_size_exceeded = true;
        }

        this->flushRawMoves();

        if (RFSD_OK != this->doCommitTransaction())
            return RFSD_FAIL;
        this->transaction.batch_running = false;
    }

    this->transaction.running = false;
    return RFSD_OK;
}

void
FsJournal::flushRawMoves()
{
    std::map<uint32_t, Block *> write_map;
    for (movemap_t::const_iterator it = this->raw_moves.begin(); it != this->raw_moves.end();
         ++ it)
    {
        // movemap is a std::map and therefore iterates in sorted manner,
        // and reads performed in sorted order. That reduces disk seeks, so reads
        // become a bit faster
        Block *block_obj = this->readBlock(it->first, false);
        this->deleteFromCache(it->first);
        block_obj->block = it->second;
        block_obj->markDirty();
        write_map[it->second] = block_obj;
    }

    for (std::map<uint32_t, Block *>::const_iterator it = write_map.begin();
        it != write_map.end(); ++ it)
    {
        // write order is sorted too
        this->releaseBlock(it->second, false);
    }

    this->raw_moves.clear();
}

int
FsJournal::flushTransactionCache()
{
    if (this->transaction.batch_running) {
        this->flushRawMoves();
        if (RFSD_OK != this->doCommitTransaction())
            return RFSD_FAIL;
        this->transaction.batch_running = false;
    }
    return RFSD_OK;
}

uint32_t
FsJournal::estimateTransactionSize()
{
    return this->transaction.blocks.size();
}

Block*
FsJournal::readBlock(uint32_t block_idx, bool caching)
{
    // check if cache have this block
    if (this->blockInCache(block_idx)) {
        this->cache_hits ++;
        this->touchCacheEntry(block_idx);
        this->block_cache[block_idx].block_obj->ref_count ++;
        return this->block_cache[block_idx].block_obj;
    }
    this->cache_misses ++;

    // not found, read from disk
    Block *block_obj = new Block();
    if (RFSD_OK != readBufAt(this->fd, block_idx, block_obj->buf, BLOCKSIZE)) {
        delete block_obj;
        return NULL;
    }
    block_obj->block = block_idx;

    if (caching) this->pushToCache(block_obj);
    return block_obj;
}

void
FsJournal::readBlock(Block &block_obj, uint32_t block_idx)
{
    readBufAt(this->fd, block_idx, block_obj.buf, BLOCKSIZE);
    block_obj.block = block_idx;
}

int
FsJournal::writeBlock(Block *block_obj, bool factor_into_trasaction)
{
    if (this->use_journaling && factor_into_trasaction) {
        if (this->transaction.blocks.count(block_obj) == 0) {
            block_obj->ref_count ++;
            this->transaction.blocks.insert(block_obj);
        }
        // must retain block until transaction ends. Further readBlocks should get
        // cached version, as disk contents differs from memory.
        this->pushToCache(block_obj, CACHE_PRIORITY_HIGH);
    } else {
        if (RFSD_OK != writeBufAt(this->fd, block_obj->block, block_obj->buf, BLOCKSIZE))
            return RFSD_FAIL;
    }
    block_obj->dirty = false;
    return RFSD_OK;
}

void
FsJournal::moveRawBlock(uint32_t from, uint32_t to, bool factor_into_trasaction)
{
    if (factor_into_trasaction) {
        Block *block_obj = this->readBlock(from, false);
        this->deleteFromCache(block_obj->block);
        // ref_count must be 1 or 2. 2 in case block was in transaction batch, 1 otherwise
        assert1 (block_obj->ref_count == 1 || block_obj->ref_count == 2);
        block_obj->block = to;
        block_obj->markDirty();
        // as we moving to free position, there can be no block
        assert1 (this->block_cache.count(block_obj->block) == 0);
        this->pushToCache(block_obj);

        if (this->transaction.blocks.count(block_obj) == 0) {
            this->transaction.blocks.insert(block_obj);
            block_obj->ref_count ++;
        }
        this->releaseBlock(block_obj, true);
    } else {    // collect raw moves
        this->raw_moves[from] = to;
        if (this->blockInCache(from)) {
            assert2("unformatted blocks should not be cached", false);
        }
    }
}

void
FsJournal::releaseBlock(Block *block_obj, bool factor_into_transaction)
{
    if (block_obj->dirty)
        this->writeBlock(block_obj, factor_into_transaction);

    block_obj->ref_count --;
    assert1 (block_obj->ref_count >= 0);
    if (block_obj->ref_count == 0)
        delete block_obj;
}

void
FsJournal::pushToCache(Block *block_obj, int priority)
{
    if (this->block_cache.size() >= this->max_cache_size - 1)
        this->eraseOldestCacheEntry();

    FsJournal::cache_entry ci;
    ci.block_obj = block_obj;
    ci.priority = priority;
    // If this block not in cache, increase ref_count.
    // There is not reason increase it if block already in cache.
    if (this->block_cache.count(block_obj->block) == 0) block_obj->ref_count ++;
    // create (or update) cache entry
    this->block_cache[block_obj->block] = ci;
}

void
FsJournal::touchCacheEntry(uint32_t block_idx)
{
    (void)block_idx; // not used
}

void
FsJournal::eraseOldestCacheEntry()
{
    // use Random Replacement
    std::map<uint32_t, cache_entry>::iterator it, dit;
    it = this->block_cache.begin();
    while (it != this->block_cache.end()) {
        dit = it ++;
        if (std::rand()%256 == 0) {
            if (dit->second.priority == CACHE_PRIORITY_NORMAL)
                this->deleteFromCache(dit->first);
        }
    }
}

void
FsJournal::deleteFromCache(uint32_t block_idx)
{
    if (this->block_cache.count(block_idx) > 0) {
        Block *block_obj = this->block_cache[block_idx].block_obj;
        this->releaseBlock(block_obj, false);
        this->block_cache.erase(block_idx);
    }
}
