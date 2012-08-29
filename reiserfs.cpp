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
ReiserFs::open(std::string name)
{
    std::cout << "open " << name << std::endl;
    this->fname = name;
    fd = ::open(name.c_str(), O_RDWR | O_SYNC | O_LARGEFILE);
    if (-1 == fd) {
        std::cerr << "error: can't open file `" << name << "', errno = " << errno << std::endl;
        return RFSD_FAIL;
    }

    this->readSuperblock();
    // this->dumpSuperblock();
    this->journal = new FsJournal(this->fd);
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

void
ReiserFs::moveMultipleBlocks(std::map<uint32_t, uint32_t> & movemap)
{
    std::cout << "ReiserFs::moveMultipleBlocks stub" << std::endl;

    std::map<uint32_t, uint32_t>::iterator iter;
    for(iter = movemap.begin(); iter != movemap.end(); ++ iter) {
        std::cout << "from: " << iter->first << ", to: ";
        std::cout << iter->second << std::endl;
    }

    // do walk tree

    std::cout << "root block: " << this->sb.s_root_block << std::endl;

    Block *root_block = this->readBlock(this->sb.s_root_block);
    root_block->setType(BLOCKTYPE_INTERNAL);
    // root_block->formattedDump();
    root_block->walk_tree();
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
