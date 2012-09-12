#pragma once
#define _FILE_OFFSET_BITS 64
#include <stdint.h>
#include <string>
#include <ostream>
#include <map>
#include <vector>
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

#define KEY_V0      0
#define KEY_V1      1

#define KEY_TYPE_STAT       0
#define KEY_TYPE_INDIRECT   1
#define KEY_TYPE_DIRECT     2
#define KEY_TYPE_DIRECTORY  3
#define KEY_TYPE_ANY        15

#define BLOCKSIZE   4096
#define BLOCKS_PER_BITMAP   (BLOCKSIZE*8)
#define SUPERBLOCK_BLOCK    (65536/BLOCKSIZE)
#define FIRST_BITMAP_BLOCK  (65536/BLOCKSIZE + 1)

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
class FsBitmap;

class Block {
public:
    Block(FsJournal *journal = NULL);
    ~Block();
    void rawDump() const;
    void formattedDump() const;
    void setType(int type) { this->type = type; }
    void write();
    void attachJournal(FsJournal *journal) { this->journal = journal; }
    void markDirty() { this->dirty = true; }
    uint32_t keyCount() const {
        const struct blockheader *bh = reinterpret_cast<const struct blockheader *>(&buf[0]);
        return bh->bh_nr_items;
    }
    uint32_t ptrCount() const {
        const struct blockheader *bh = reinterpret_cast<const struct blockheader *>(&buf[0]);
        return bh->bh_nr_items + 1;
    }
    uint32_t level() const {
        const struct blockheader *bh = reinterpret_cast<const struct blockheader *>(&buf[0]);
        return bh->bh_level;
    }
    uint32_t freeSpace() const {
        const struct blockheader *bh = reinterpret_cast<const struct blockheader *>(&buf[0]);
        return bh->bh_free_space;
    }
    uint32_t itemCount() const {
        const struct blockheader *bh = reinterpret_cast<const struct blockheader *>(&buf[0]);
        return bh->bh_nr_items;
    }
    void dumpInternalNodeBlock() const;
    void dumpLeafNodeBlock() const;
    const uint32_t &indirectItemRef(uint16_t offset, uint32_t idx) const {
        const uint32_t *ci = reinterpret_cast<uint32_t const *>(&buf[0] + offset + 4*idx);
        const uint32_t &ref = ci[0];
        return ref;
    }
    void setIndirectItemRef(uint16_t offset, uint32_t idx, uint32_t value) {
        uint32_t *ci = reinterpret_cast<uint32_t *>(&buf[0] + offset + 4*idx);
        ci[0] = value;
        this->dirty = true;
    }

    uint32_t block;
    int type;
    char buf[BLOCKSIZE];
    bool dirty;
    int32_t ref_count;
    FsJournal *journal;

    struct blockheader {
        uint16_t bh_level;
        uint16_t bh_nr_items;
        uint16_t bh_free_space;
        uint16_t bh_reserved1;
        uint8_t bh_right_key[16];
    } __attribute__ ((__packed__));

    // reiserfs key, 3.5 (v0) and 3.6 (v1) formats
    struct key {
        uint32_t dir_id;
        uint32_t obj_id;
        uint32_t offset_type_1;
        uint32_t offset_type_2;

        uint32_t offset_v0() const { return offset_type_1; }
        uint64_t offset_v1() const {
            return (static_cast<uint64_t>(offset_type_2 & 0x0FFFFFFF) << 32) + offset_type_1;
        }
        uint32_t type_v0() const { return offset_type_2; }
        uint32_t type_v1() const { return (offset_type_2 & 0xF0000000) >> 28; }
        void dump_v0(std::ostream &stream, bool need_endl = false) const {
            stream << "{" << this->dir_id << ", " << this->obj_id << ", ";
            stream << this->offset_v0() << ", " << this->type_v0() << "}";
            if (need_endl) stream << std::endl;
        }
        void dump_v1(std::ostream &stream, bool need_endl = false) const {
            stream << "{" << this->dir_id << ", " << this->obj_id << ", ";
            stream << this->offset_v1() << ", " << this->type_v1() << "}";
            if (need_endl) stream << std::endl;
        }
        static const char *type_name(int type) {
            switch (type) {
            case KEY_TYPE_STAT: return "stat"; break;
            case KEY_TYPE_INDIRECT: return "indirect"; break;
            case KEY_TYPE_DIRECT: return "direct"; break;
            case KEY_TYPE_DIRECTORY: return "directory"; break;
            case KEY_TYPE_ANY: return "any"; break;
            default: return "wrong item";
            }
        }
        int type(int key_version) const {
            switch (key_version) {
            case KEY_V0: {
                switch (this->type_v0()) {
                case 0:          return KEY_TYPE_STAT; break; // stat
                case 0xfffffffe: return KEY_TYPE_INDIRECT; break; // indirect
                case 0xffffffff: return KEY_TYPE_DIRECT; break; // direct
                case 500:        return KEY_TYPE_DIRECTORY; break; // directory
                case 555:        return KEY_TYPE_ANY; break; // any
                default: return 16; break; // TODO: add code for this case
                }
            }
            case KEY_V1: return this->type_v1(); break;
            default: // TODO: add code for this case
                return 16;
            }

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
        int type() const { return this->key.type(this->version); }
    } __attribute__ ((__packed__));

    const struct key &key(uint32_t index) const {
        const struct key *kp = reinterpret_cast<const struct key *>(&buf[0] + 24 + 16*index);
        const struct key &kpr = kp[0];
        return kpr;
    }
    const struct tree_ptr &ptr(uint32_t index) const {
        const struct tree_ptr *tp =
            reinterpret_cast<const struct tree_ptr *>(&buf[0] + 24 + 16*keyCount() + 8*index);
        const struct tree_ptr &tpr = tp[0];
        return tpr;
    }
    struct tree_ptr &ptr(uint32_t index) {
        struct tree_ptr *tp =
            reinterpret_cast<struct tree_ptr *>(&buf[0] + 24 + 16*keyCount() + 8*index);
        struct tree_ptr &tpr = tp[0];
        return tpr;
    }
    const struct item_header &itemHeader(uint32_t index) const {
        const struct item_header *ihp =
            reinterpret_cast<const struct item_header*>(&buf[0] + 24 + 24*index);
        const struct item_header &ihpr = ihp[0];
        return ihpr;
    }
};

class FsJournal {
public:
    FsJournal(int fd_);
    ~FsJournal();
    Block* readBlock(uint32_t block_idx, bool caching = true);
    void readBlock(Block &block_obj, uint32_t block_idx);
    void writeBlock(Block *block_obj);
    void releaseBlock(Block *block_obj);
    void moveRawBlock(uint32_t from, uint32_t to);
    void beginTransaction();
    void commitTransaction();

private:
    struct cache_entry {
        Block *block_obj;
    };
    int fd;
    std::map<uint32_t, cache_entry> block_cache;
    int64_t cache_hits;
    int64_t cache_misses;
    uint32_t max_cache_size;

    bool blockInCache(uint32_t block_idx) { return this->block_cache.count(block_idx) > 0; }
    void pushToCache(Block *block_obj);
    void deleteFromCache(uint32_t block_idx);
    void touchCacheEntry(uint32_t block_idx);
    void eraseOldestCacheEntry();

};

class FsBitmap {
public:
    FsBitmap(FsJournal *journal, const FsSuperblock *sb);
    ~FsBitmap();
    bool blockUsed(uint32_t block_idx) const;
    void markBlockUsed(uint32_t block_idx);
    void markBlockFree(uint32_t block_idx);
    void markBlock(uint32_t block_idx, bool used);

private:
    FsJournal *journal;
    const FsSuperblock *sb;
    std::vector<Block> bitmap_blocks;
};

class ReiserFs {
public:
    typedef struct {
        uint32_t type;
        uint32_t idx;
    } tree_element;

    ReiserFs();
    ~ReiserFs();
    int open(const std::string &name, bool o_sync = true);
    void close();
    void moveBlock(uint32_t from, uint32_t to);
    uint32_t moveMultipleBlocks(std::map<uint32_t, uint32_t> & movemap);
    void dumpSuperblock();

    // proxies for FsJournal methods
    Block* readBlock(uint32_t block) const;
    void releaseBlock(Block *block) const;

    void printFirstFreeBlock();
    uint32_t findFreeBlockBefore(uint32_t block_idx) const;
    uint32_t findFreeBlockAfter(uint32_t block_idx) const;
    bool blockUsed(uint32_t block_idx) const { return this->bitmap->blockUsed(block_idx); }
    uint32_t sizeInBlocks() const { return this->sb.s_block_count; }
    void looseWalkTree();
    std::vector<tree_element> *enumerateTree() const;

    /// checks if block is bitmap
    bool blockIsBitmap(uint32_t block_idx) const;
    bool blockIsJournal(uint32_t block_idx) const;
    bool blockIsFirst64k(uint32_t block_idx) const;
    bool blockIsSuperblock(uint32_t block_idx) const;
    /// checks if block is in reserved area, such as journal, sb, bitmap of first 64kiB
    bool blockReserved(uint32_t block_idx) const;

private:
    FsBitmap *bitmap;
    FsJournal *journal;
    FsSuperblock sb;
    std::string fname;
    int fd;
    bool closed;
    std::string err_string;
    uint32_t blocks_moved_formatted;    //< counter used for moveMultipleBlocks
    uint32_t blocks_moved_unformatted;  //< counter used for moveMultipleBlocks

    void readSuperblock();
    bool movemap_consistent(const std::map<uint32_t, uint32_t> &movemap);
    void collectLeafNodeIndices(uint32_t block_idx, std::vector<uint32_t> &lni);
    void recursivelyMoveInternalNodes(uint32_t block_idx, std::map<uint32_t, uint32_t> &movemap,
        uint32_t target_level);
    void recursivelyMoveUnformatted(uint32_t block_idx, std::map<uint32_t, uint32_t> &movemap);
    uint32_t estimateTreeHeight();
    void recursivelyEnumerateNodes(uint32_t block_idx, std::vector<ReiserFs::tree_element> &tree) const;
};
