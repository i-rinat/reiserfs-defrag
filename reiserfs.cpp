#include "reiserfs.hpp"
#include <iostream>
#include <map>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

ReiserFs::ReiserFs()
{
    this->closed = true;
}

ReiserFs::~ReiserFs()
{
    if (! this->closed) this->close();
}

int
ReiserFs::open(const std::string &name)
{
    std::cout << "open " << name << std::endl;
    this->fname = name;
    fd = ::open(name.c_str(), O_RDWR | O_SYNC | O_LARGEFILE);
    if (-1 == fd) {
        std::cerr << "error: can't open file `" << name << "', errno = " << errno << std::endl;
        return RFSD_FAIL;
    }

    this->readSuperblock();
    this->journal = new FsJournal(this->fd);
    this->bitmap = new FsBitmap(this->journal, &this->sb);
    this->closed = true;

    return RFSD_OK;
}

void
ReiserFs::readSuperblock()
{
    std::cout << "readSuperblock" << std::endl;
    const off_t sb_offset = 65536;
    off_t res = ::lseek (this->fd, sb_offset, SEEK_SET);
    if (res != sb_offset) {
        std::cerr << "error: can't lseek, errno = " << errno << std::endl;
        return;
    }

    char buf[4096];
    ssize_t bytes_read = ::read (this->fd, buf, sizeof(buf));
    if (bytes_read == -1) {
        std::cerr << "error: can't read, errno = " << errno << ", " <<
            strerror(errno) << std::endl;
        std::cout << sizeof(buf) << std::endl;
        std::cout << this->fd << std::endl;
        return;
    }

    memcpy(&this->sb, buf, sizeof(this->sb));
}

void
ReiserFs::dumpSuperblock()
{
    std::cout << "dumpSuperblock() --------------------------------------" <<
        std::endl;
    std::cout << "block count = " << sb.s_block_count << std::endl;
    std::cout << "free block count = " << sb.s_free_blocks << std::endl;
    std::cout << "root block at = " << sb.s_root_block << std::endl;
    std::cout << "journal start = " << sb.jp_journal_1st_block << std::endl;
    std::cout << "journal dev = " << sb.jp_journal_dev << std::endl;
    std::cout << "journal size = " << sb.jp_journal_size << std::endl;
    std::cout << "journal max transactions = " << sb.jp_journal_trans_max <<
        std::endl;
    std::cout << "journal magic = " << sb.jp_journal_magic << std::endl;
    std::cout << "journal max batch = " << sb.jp_journal_max_batch <<std::endl;
    std::cout << "journal max commit age = " << sb.jp_journal_max_commit_age <<
        std::endl;
    std::cout << "journal max transaction age = " <<
        sb.jp_journal_max_trans_age << std::endl;
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
    std::cout << "size of journal area = " << sb.s_reserved_for_journal <<
        std::endl;
    std::cout << "inode generation = " << sb.s_inode_generation << std::endl;
    std::cout << "flags = " << sb.s_flags << std::endl;
    std::cout << "uuid = not implemented" << std::endl;
    std::cout << "label = not implemented" << std::endl;
    std::cout << "mount count = " << sb.s_mnt_count << std::endl;
    std::cout << "max mount count = " << sb.s_max_mnt_count << std::endl;
    std::cout << "last check = " << sb.s_lastcheck << std::endl;
    std::cout << "check interval = " << sb.s_check_interval << std::endl;
    std::cout << "unused fields dump = not implemented" << std::endl;

    std::cout << "=======================================================" <<
        std::endl;
}

void
ReiserFs::close()
{
    std::cout << "ReiserFs::close, " << this->fname << std::endl;
    delete this->bitmap;
    delete this->journal;
    ::close(this->fd);
    this->closed = true;
}

void
ReiserFs::moveBlock(uint32_t from, uint32_t to)
{
    std::map<uint32_t, uint32_t> movemap;
    movemap[from] = to;
    this->moveMultipleBlocks (movemap);
}

bool
ReiserFs::movemap_consistent(const std::map<uint32_t, uint32_t> &movemap)
{
    std::map<uint32_t, uint32_t> revmap;
    std::map<uint32_t, uint32_t>::const_iterator mapiter;

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

void
ReiserFs::moveMultipleBlocks(std::map<uint32_t, uint32_t> & movemap)
{
    if (! this->movemap_consistent(movemap)) {
        std::cerr << "error: movemap not consistent, " << this->err_string << std::endl;
        return;
    }

    std::map<uint32_t, uint32_t>::const_iterator iter;
    for(iter = movemap.begin(); iter != movemap.end(); ++ iter) {
        std::cout << "from: " << iter->first << ", to: ";
        std::cout << iter->second << std::endl;
    }
    std::cout << "root block: " << this->sb.s_root_block << std::endl;

    Block *root_block = this->readBlock(this->sb.s_root_block);
    root_block->setType(BLOCKTYPE_INTERNAL);
    this->walk_tree(root_block, movemap);
    this->releaseBlock(root_block);
}


Block*
ReiserFs::readBlock(uint32_t block)
{
    assert(this->journal != NULL);
    return this->journal->readBlock(block);
}

void
ReiserFs::releaseBlock(Block *block)
{
    assert(this->journal != NULL);
    journal->releaseBlock(block);
}

void
ReiserFs::walk_tree(Block *block_obj, std::map<uint32_t, uint32_t> &movemap)
{
    if (block_obj->type == BLOCKTYPE_INTERNAL) {
        for (int k = 0; k < block_obj->ptrCount(); k ++) {
            Block *child_block = this->journal->readBlock(block_obj->getPtr(k).block);
            if (child_block->level() > TREE_LEVEL_LEAF) {
                child_block->setType(BLOCKTYPE_INTERNAL);
                this->walk_tree(child_block, movemap);
            } else if (child_block->level() == TREE_LEVEL_LEAF) {
                child_block->setType(BLOCKTYPE_LEAF);
                // process leaf contents
                for (int j = 0; j < child_block->itemCount(); j ++) {
                    const struct Block::item_header &ih = child_block->itemHeader(j);

                    uint32_t key_version = ih.version;
                    uint32_t item_type = ih.key.type(ih.version);

                    // indirect items contain links to unformatted (data) blocks
                    if (KEY_TYPE_INDIRECT == item_type) {
                        for (int idx = 0; idx < ih.length/4; idx ++) {
                            uint32_t ref = child_block->indirectItemRef(ih.offset, idx);
                            if (movemap.count(ref) == 0) continue;
                            // we have something to move
                            std::cout << "we have something to move" << std::endl;
                            // update indirect block
                            child_block->setIndirectItemRef(ih.offset, idx, movemap[ref]);
                            // actually move block
                            this->journal->moveRawBlock(ref, movemap[ref]);
                            // update bitmap
                            this->bitmap->markBlockUnused(ref);
                            this->bitmap->markBlockUsed(movemap[ref]);
                        }
                    }
                }
            } else {
                std::cerr << "error: unknown block in tree" << std::endl;
            }

            // TODO: do move internal block

            delete child_block;
        }
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
ReiserFs::findFreeBlockAfter(uint32_t block_idx)
{
    for (uint32_t k = block_idx + 1; k < this->sb.s_block_count; k ++)
        if (! this->bitmap->blockUsed(k)) return k;
    return 0;
}

uint32_t
ReiserFs::findFreeBlockBefore(uint32_t block_idx)
{
    // lost 0th block, but it's reserved anyway
    for (uint32_t k = block_idx - 1; k > 0; k ++)
        if (! this->bitmap->blockUsed(k)) return k;
    return 0;
}
