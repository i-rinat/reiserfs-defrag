#pragma once
#define _FILE_OFFSET_BITS 64
#include <stdint.h>
#include <string>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define RFSD_OK     0
#define RFSD_FAIL   -1

#define BLOCKSIZE   4096

struct FsSuperblock {
    uint32_t s_block_count;
    uint32_t s_free_blocks;
    uint32_t s_root_block;

    // from struct journal_params
    uint32_t jp_journal_1st_block;
    uint32_t jp_journal_dev;
    uint32_t jp_journal_size;
    uint32_t jp_journal_trans_max;
    uint32_t jp_journal_magic;
    uint32_t jp_journal_max_batch;
    uint32_t jp_journal_max_commit_age;
    uint32_t jp_journal_max_trans_age;

    uint16_t s_blocksize;
    uint16_t s_oid_maxsize;
    uint16_t s_oid_cursize;
    uint16_t s_umount_state;
    char s_magic[10];
    uint16_t s_fs_state;
    uint16_t s_hash_function_code;
    uint16_t s_tree_height;
    uint16_t s_bmap_nr;
    uint16_t s_version;
    uint16_t s_reserved_for_journal;

    // end of v1 superblock, v2 additions below
    uint32_t s_inode_generation;
    uint32_t s_flags;
    uint8_t s_uuid[16];
    char s_label[16];
    uint16_t s_mnt_count;
    uint16_t s_max_mnt_count;
    uint32_t s_lastcheck;
    uint32_t s_check_interval;
    uint8_t s_unused[76];

} __attribute__ ((__packed__));

class Block {
public:
    Block();
    const char *ptr() const { return &buf[0]; }
    char *ptr() { return &buf[0]; }
    uint32_t block;
private:
    char buf[BLOCKSIZE];
};

class FormattedBlock : public Block {
public:
};

class UnformattedBlock : public Block {
public:
};

class FsJournal {
public:
    FsJournal(int fd_);
    Block* readBlock(uint32_t block);
    void dumpBlock(const Block *block) const;
    void releaseBlock(Block *block);
    void beginTransaction();
    void commitTransaction();

private:
    int fd;

};

class FsBitmap {
public:
};

class ReiserFs {
public:
    ReiserFs();
    ~ReiserFs();
    int open(std::string name);
    void close();
    void moveBlock(uint32_t from, uint32_t to);
    void moveMultipleBlocks(std::map<uint32_t, uint32_t> movemap);
    void dumpSuperblock();

    // proxies for FsJournal methods
    Block* readBlock(uint32_t block);
    void dumpBlock(const Block *block) const;
    void releaseBlock(Block *block);



private:
    FsBitmap bitmap;
    FsJournal *journal;
    FsSuperblock sb;
    std::string fname;
    int fd;

    bool closed;

    void readSuperblock();
};
