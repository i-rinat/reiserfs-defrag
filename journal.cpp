#include "reiserfs.hpp"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdlib>
#include <assert.h>

FsJournal::FsJournal(int fd_)
{
    this->fd = fd_;
    std::cout << "FsJournal initialized" << std::endl;
    this->generation = 0;
    this->max_cache_size = 5120;
}

FsJournal::~FsJournal()
{
    std::cout << "~FsJournal, block_cache.size() = " << this->block_cache.size() << std::endl;
    // purge cache will be performed automatically
    std::map<uint32_t, cache_entry>::iterator it, dit;
    it = this->block_cache.begin();
    while (it != this->block_cache.end()) {
        dit = it ++;
        dit->second.block_obj->marked_for_gc = true;
        this->deleteFromCache(dit->first);
    }
    if (this->block_cache.size() > 0) {
        std::cerr << "error: FsJournal::block_cache is not empty" << std::endl;
        assert (false);
    }
}

void
FsJournal::beginTransaction()
{
    // std::cout << "FsJournal::beginTransaction stub" << std::endl;
}

void
FsJournal::commitTransaction()
{
    // std::cout << "FsJournal::commitTransaction stub" << std::endl;
    // ::fsync(this->fd);
}

Block*
FsJournal::readBlock(uint32_t block_idx)
{
    this->generation ++;
    // check if cache have this block
    if (this->blockInCache(block_idx)) {
        this->touchCacheEntry(block_idx);
        return this->block_cache[block_idx].block_obj;
    }
    // not found, read from disk
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_idx) * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return NULL;
    }
    Block *block_obj = new Block(this);
    ssize_t bytes_read = ::read (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock(" << block_idx << ")" << std::endl;
        return NULL;
    }
    block_obj->block = block_idx;
    this->pushToCache(block_obj);
    return block_obj;
}

void
FsJournal::readBlock(Block &block_obj, uint32_t block_idx)
{
    off_t new_ofs = ::lseek (this->fd, (off_t)block_idx * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return;
    }
    ssize_t bytes_read = ::read (this->fd, block_obj.buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_read) {
        std::cerr << "error: readBlock(" << &block_obj << ", " << block_idx << ")" << std::endl;
        return;
    }
    block_obj.block = block_idx;
}

void
FsJournal::writeBlock(Block *block_obj)
{
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

void
FsJournal::writeBlockAt(Block *block_obj, uint32_t block_idx)
{
    this->deleteFromCache(block_obj->block);
    off_t new_ofs = ::lseek (this->fd, static_cast<off_t>(block_idx) * BLOCKSIZE, SEEK_SET);
    if (static_cast<off_t>(-1) == new_ofs) {
        std::cerr << "error: seeking" << std::endl;
        // TODO: error handling
        return;
    }
    ssize_t bytes_written = ::write (this->fd, block_obj->buf, BLOCKSIZE);
    if (BLOCKSIZE != bytes_written) {
        std::cerr << "error: writeBlockAt(" << block_obj << ", " << block_idx << ")" << std::endl;
        return;
    }
    // push same block under new idx to cache
    block_obj->block = block_idx;
    this->pushToCache(block_obj);
}

void
FsJournal::moveRawBlock(uint32_t from, uint32_t to)
{
    Block *block_obj = this->readBlock(from);
    this->writeBlockAt(block_obj, to);
    this->releaseBlock(block_obj);
}

void
FsJournal::releaseBlock(Block *block_obj)
{
    if (block_obj->dirty) {
        this->writeBlock(block_obj);
        block_obj->dirty = false;
    }
    block_obj->marked_for_gc = true;
    if (this->block_cache.count(block_obj->block) == 0)
        delete block_obj;
}

void
FsJournal::pushToCache(Block *block_obj)
{
    while (this->block_cache.size() >= this->max_cache_size - 1)
        this->eraseOldestCacheEntry();

    FsJournal::cache_entry ci;
    ci.block_obj = block_obj;
    ci.generation = this->generation;
    this->block_cache[block_obj->block] = ci;
}

void
FsJournal::eraseOldestCacheEntry()
{
    // use Random Replacement
    std::map<uint32_t, cache_entry>::iterator it, dit;
    it = this->block_cache.begin();
    while (it != this->block_cache.end()) {
        dit = it ++;
        if (std::rand()%2 == 0) {
            this->deleteFromCache(dit->first);
        }
    }
}

void
FsJournal::deleteFromCache(uint32_t block_idx)
{
    Block *block_obj = this->block_cache[block_idx].block_obj;
    if (block_obj->marked_for_gc) delete block_obj;
    this->block_cache.erase(block_idx);
}
