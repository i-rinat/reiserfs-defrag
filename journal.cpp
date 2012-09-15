#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <algorithm>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdlib>
#include <assert.h>

FsJournal::FsJournal(int fd_, FsSuperblock *sb)
{
    this->fd = fd_;
    this->cache_hits = 0;
    this->cache_misses = 0;
    this->max_cache_size = 51200;
    this->transaction.running = false;
    this->use_journaling = true;
    this->sb = sb;
}

FsJournal::~FsJournal()
{
    // purge cache will be performed automatically
    std::map<uint32_t, cache_entry>::iterator it, dit;
    it = this->block_cache.begin();
    while (it != this->block_cache.end()) {
        dit = it ++;
        this->deleteFromCache(dit->first);
    }
    if (this->block_cache.size() > 0) {
        std::cerr << "error: FsJournal::block_cache is not empty" << std::endl;
        assert (false);
    }
    std::cout << "cache hits = " << this->cache_hits <<
        ", cache_misses = " << this->cache_misses << std::endl;
}

void
FsJournal::beginTransaction()
{
    if (not this->use_journaling) return;

    if (this->transaction.running) {
        std::cerr << "error: nested transaction" << std::endl;
        return; // TODO: error handling
    }
    if (this->transaction.blocks.size() != 0) {
        std::cerr << "error: there was writes outside transaction" << std::endl;
        return; // TODO: error handling
    }

    this->transaction.running = true;
}

inline
int
writeBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size)
{
    off_t ofs = static_cast<off_t>(block_idx) * BLOCKSIZE;
    off_t new_ofs = ::lseek (fd, ofs, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs || new_ofs != ofs) {
        std::cerr << "error: can't seek to " << ofs << "." << std::endl;
        return RFSD_FAIL;
    }
    ssize_t bytes_written = ::write (fd, buf, size);
    if (size != bytes_written) {
        std::cerr << "error: can't write data at block " << block_idx << "." << std::endl;
        return RFSD_FAIL;
    }
    return RFSD_OK;
}

inline
int
readBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size)
{
    off_t ofs = static_cast<off_t>(block_idx) * BLOCKSIZE;
    off_t new_ofs = ::lseek (fd, ofs, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs || new_ofs != ofs) {
        std::cerr << "error: can't seek to " << ofs << "." << std::endl;
        return RFSD_FAIL;
    }
    ssize_t bytes_read = ::read (fd, buf, size);
    if (size != bytes_read) {
        std::cerr << "error: can't read data at block " << block_idx << "." << std::endl;
        return RFSD_FAIL;
    }
    return RFSD_OK;
}

void
FsJournal::removeDuplicateTransactionEntries()
{
    // first, sort them
    std::sort (this->transaction.blocks.begin(), this->transaction.blocks.end());
    // remove duplicates
    std::vector<Block *>::iterator prev = this->transaction.blocks.begin();
    for (std::vector<Block *>::iterator it = prev+1; it != this->transaction.blocks.end(); ++it) {
        if (*prev == *it) {
            (*prev)->ref_count --;  // duplicate, as we 'removing' it, decrease reference count
            assert ((*prev)->ref_count > 0);    // anyway there should be at least one reference
        } else {
            ++ prev;        // advance pointer
            *prev = *it;    // and move right element to left
        }
    }
    // prev points to last unique element. erase trailing
    this->transaction.blocks.erase (prev+1, this->transaction.blocks.end());

    // check if any of blocks are dirty. They all must be not.
    for (std::vector<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        assert ((*it)->dirty == false); // must not be dirty
    }
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
    uint32_t transaction_block_count = this->transaction.blocks.size();

    // check for proper alignment, just to be sure
    assert (sizeof(commit_block) == BLOCKSIZE);
    assert (sizeof(description_block) == BLOCKSIZE);

    // fill description and commit blocks
    description_block.transaction_id = transaction_id;
    description_block.length = this->transaction.blocks.size();
    description_block.mount_id = journal_header.mount_id;
    memcpy (description_block.magic, "ReIsErLB", 8);
    commit_block.transaction_id = transaction_id;
    commit_block.length = this->transaction.blocks.size();

    uint32_t first_half = std::min(static_cast<uint32_t>((BLOCKSIZE-24)/4),
                                    transaction_block_count);
    for (uint32_t k = 0; k < first_half; k ++)
        description_block.real_blocks[k] = this->transaction.blocks[k]->block;
    if (transaction_block_count > first_half) {
        for (uint32_t k = first_half; k < transaction_block_count; k ++)
            commit_block.real_blocks[k - first_half] = this->transaction.blocks[k]->block;
    }

    uint32_t j_pos = transaction_offset; // cursor
    const uint32_t j_1st_block = this->sb->jp_journal_1st_block;

    // write desc block
    if (RFSD_OK != writeBufAt (this->fd, j_1st_block + j_pos, &description_block, BLOCKSIZE) )
        return RFSD_FAIL;
    j_pos = (j_pos + 1) % this->sb->jp_journal_size;

    // write data blocks
    for (std::vector<Block *>::const_iterator it = this->transaction.blocks.begin();
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
FsJournal::commitTransaction()
{
    if (not this->use_journaling) return RFSD_OK;

    if (this->transaction.blocks.size() == 0) {
        // std::cout << "warning: empty transaction" << std::endl;
        this->transaction.running = false;
        return RFSD_OK;
    }

    // remove duplicate entries
    this->removeDuplicateTransactionEntries();

    std::cout << "Journal: transaction size = " << this->transaction.blocks.size() << std::endl;

    // read journal header
    readBufAt ( this->fd, this->sb->jp_journal_1st_block + this->sb->jp_journal_size,
                &journal_header, sizeof(journal_header));

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
    for (std::vector<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        if (RFSD_OK != writeBufAt(this->fd, (*it)->block, (*it)->buf, BLOCKSIZE))
            return RFSD_FAIL;
    }

    // finally release blocks. Block can survive this if it has more than one reference,
    // like cached blocks. ->releaseBlock will not call writeBlock as block is not
    // dirty.
    for (std::vector<Block *>::const_iterator it = this->transaction.blocks.begin();
        it != this->transaction.blocks.end(); ++ it)
    {
        uint32_t block_idx = (*it)->block;
        // reset block priority to normal. Contents written to disk, so cache entry
        // may safelly be deleted if needed
        if (this->block_cache.count(block_idx) > 0)
            this->block_cache[block_idx].priority = CACHE_PRIORITY_NORMAL;
        this->releaseBlock(*it);
    }
    this->transaction.blocks.resize(0);

    // sync journal header, thus closing transaction
    int res = writeBufAt (this->fd, this->sb->jp_journal_1st_block + this->sb->jp_journal_size,
        &journal_header, sizeof(journal_header));
    if (RFSD_OK != res)
        return RFSD_FAIL;

    this->transaction.running = false;
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
        if (caching) this->cache_hits ++;
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

void
FsJournal::writeBlock(Block *block_obj)
{
    if (this->use_journaling) {
        this->transaction.blocks.push_back(block_obj);
        block_obj->ref_count ++;
        // must retain block until transaction ends. Further readBlocks should get
        // cached version, as disk contents differs from memory.
        this->pushToCache(block_obj, CACHE_PRIORITY_HIGH);
    } else {
        off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_obj->block) * BLOCKSIZE, SEEK_SET);
        if (static_cast<off_t>(-1) == new_ofs) {
            std::cerr << "error: seeking" << std::endl;
            // TODO: error handling
            return;
        }
        ssize_t bytes_written = ::write (this->fd, block_obj->buf, BLOCKSIZE);
        if (BLOCKSIZE != bytes_written) {
            std::cerr << "error: writeBlock(" << &block_obj << ")" << std::endl;
            return;
        }
    }
    block_obj->dirty = false;
}

void
FsJournal::moveRawBlock(uint32_t from, uint32_t to, bool include_in_transaction)
{
    Block *block_obj = this->readBlock(from, false);
    this->deleteFromCache(block_obj->block);
    assert (block_obj->ref_count == 1);
    block_obj->markDirty();
    block_obj->block = to;

    if (include_in_transaction) {
        this->transaction.blocks.push_back(block_obj);
        block_obj->ref_count ++;
    }
    this->releaseBlock(block_obj);
}

void
FsJournal::releaseBlock(Block *block_obj)
{
    if (block_obj->dirty)
        this->writeBlock(block_obj);

    block_obj->ref_count --;
    assert (block_obj->ref_count >= 0);
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
        this->releaseBlock(block_obj);
        this->block_cache.erase(block_idx);
    }
}
