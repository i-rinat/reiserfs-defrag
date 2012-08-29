#pragma once
#define _FILE_OFFSET_BITS 64
#include <stdint.h>
#include <string>
#include <ostream>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define RFSD_OK     0
#define RFSD_FAIL   -1

#define BLOCKTYPE_UNKNOWN 0
#define BLOCKTYPE_INTERNAL 1
#define BLOCKTYPE_LEAF 2
#define BLOCKTYPE_UNFORMATTED 3

#define TREE_LEVEL_LEAF 1

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
    uint32_t s_hash_function_code;
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



class FsJournal;

class Block {
public:
    Block(FsJournal *journal_);
    virtual ~Block() {}
    const char *bufPtr() const { return &buf[0]; }
    char *bufPtr() { return &buf[0]; }
    void rawDump() const;
    void formattedDump() const;
    void setType(int type_);
    void walk_tree();

    uint32_t block;
protected:
    char buf[BLOCKSIZE];
    int type;
    FsJournal *journal;

    void dumpInternalNodeBlock() const;
    void dumpLeafNodeBlock() const;


    int keyCount() const;
    int ptrCount() const;
    int level() const;
    int freeSpace() const;
    int itemCount() const;

    struct blockheader {
        uint16_t bh_level;
        uint16_t bh_nr_items;
        uint16_t bh_free_space;
        uint16_t bh_reserved1;
        uint8_t bh_right_key[16];
    } __attribute__ ((__packed__));

    struct key {
        uint32_t dir_id;
        uint32_t obj_id;
        uint32_t offset_type_1;
        uint32_t offset_type_2;
        uint64_t offset() {
            return (static_cast<uint64_t>(offset_type_2 & 0x0FFFFFFF) << 32) +
                offset_type_1;
        }
        uint64_t offset() const {
            return (static_cast<uint64_t>(offset_type_2 & 0x0FFFFFFF) << 32) +
                offset_type_1;
        }
        uint32_t type() { return (offset_type_2 & 0xF0000000) >> 28; }
        uint32_t type() const { return (offset_type_2 & 0xF0000000) >> 28; }
        void dump_v2(std::ostream &stream, bool need_endl = false) const {
            stream << "{" << this->dir_id << ", " << this->obj_id << ", ";
            stream << this->offset() << ", " << this->type() << "}";
            if (need_endl) stream << std::endl;
        }
    } __attribute__ ((__packed__));

    struct tree_ptr {
        uint32_t block;
        uint16_t size;
        uint16_t reserved;
    } __attribute__ ((__packed__));

    struct item_header {
        struct key key;
        uint16_t count;
        uint16_t length;
        uint16_t offset;
        uint16_t version;
    } __attribute__ ((__packed__));

    const struct key &getKey(int index) const;
    const struct tree_ptr &getPtr(int index) const;
    const struct item_header &itemHeader(int index) const;
};

class FsJournal {
public:
    FsJournal(int fd_);
    Block* readBlock(uint32_t block);
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
    void moveMultipleBlocks(std::map<uint32_t, uint32_t> & movemap);
    void dumpSuperblock();

    // proxies for FsJournal methods
    Block* readBlock(uint32_t block);
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
