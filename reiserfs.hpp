/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#pragma once
#define _FILE_OFFSET_BITS 64
#include <stdint.h>
#include <string>
#include <ostream>
#include <map>
#include <set>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
    #error "Sorry, can't build for big endian machine"
#endif

const int RFSD_OK    = 0;
const int RFSD_FAIL  = -1;

const uint32_t BLOCKTYPE_UNKNOWN = 0;
const uint32_t BLOCKTYPE_INTERNAL = 1;
const uint32_t BLOCKTYPE_LEAF = 2;
const uint32_t BLOCKTYPE_UNFORMATTED = 3;

const uint32_t TREE_LEVEL_LEAF = 1;
const uint32_t TREE_LEVEL_MAX = 7;

const int KEY_V0 = 0;
const int KEY_V1 = 1;
const int KEY_VGUESS = 254;

const uint32_t KEY_TYPE_STAT       = 0;
const uint32_t KEY_TYPE_INDIRECT   = 1;
const uint32_t KEY_TYPE_DIRECT     = 2;
const uint32_t KEY_TYPE_DIRECTORY  = 3;
const uint32_t KEY_TYPE_ANY        = 15;

const uint32_t BLOCKSIZE = 4096;
const uint32_t BLOCKS_PER_BITMAP = BLOCKSIZE * 8;
const uint32_t BLOCKS_IN_ONE_MB = 1024*1024/BLOCKSIZE;
const uint32_t SUPERBLOCK_BLOCK = 65536 / BLOCKSIZE;
const uint32_t FIRST_BITMAP_BLOCK = 65536 / BLOCKSIZE + 1;

const int CACHE_PRIORITY_NORMAL = 0;
const int CACHE_PRIORITY_HIGH   = 1;

const uint32_t UMOUNT_STATE_CLEAN = 1;
const uint32_t UMOUNT_STATE_DIRTY = 2;

const uint32_t AG_SIZE_128M = 128*1024*1024/BLOCKSIZE;
const uint32_t AG_SIZE_256M = 256*1024*1024/BLOCKSIZE;
const uint32_t AG_SIZE_512M = 512*1024*1024/BLOCKSIZE;

// they said something about NIH
#define assert2(msg, expr)   do { \
    if (!(expr)) assert_failfunc2(msg, #expr, __FILE__, __LINE__); \
} while (0);

#define assert1(expr)   do { \
    if (!(expr)) assert_failfunc1(#expr, __FILE__, __LINE__); \
} while (0);

#define fatal(msg)  do { \
    assert_failfunc_s(msg, __FILE__, __LINE__); \
} while (0)

__attribute__((noreturn)) extern void
assert_failfunc1(const std::string &expr, const std::string &filename, int lineno);

__attribute__((noreturn)) extern void
assert_failfunc2(const std::string &msg, const std::string &expr, const std::string &filename,
                 int lineno);

__attribute__((noreturn)) extern void
assert_failfunc_s(const std::string &msg, const std::string &filename, int lineno);


typedef std::vector<uint32_t> blocklist_t;
typedef std::map<uint32_t, uint32_t> movemap_t;

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
    Block();
    ~Block();
    void rawDump() const;
    void formattedDump() const;
    void setType(int type) { this->type = type; }
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
    void checkLeafNode() const;
    void checkInternalNode() const;


    uint32_t block;
    int type;
    char __attribute__((may_alias)) buf[BLOCKSIZE];
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
    struct key_struct {
        uint32_t dir_id;
        uint32_t obj_id;
        union {
            struct {
                uint32_t offset;
                uint32_t type;
            } v0;
            struct {
                uint64_t offset : 60;
                uint64_t type : 4;
            } v1;
        };

        key_struct() {};
        key_struct(int version, uint32_t _dir, uint32_t _obj, uint64_t _ofs, uint32_t _t) {
            this->dir_id = _dir;
            this->obj_id = _obj;
            switch (version) {
            case KEY_V0:
                this->v0.offset = _ofs;
                this->v0.type = _t;
                break;
            case KEY_V1:
                this->v1.offset = _ofs;
                this->v1.type = _t;
                break;
            default:
                fatal("wrong key version");
            }
        }

        int guessVersion() const {
            if (0 == v1.type || 15 == v1.type) return KEY_V0;
            else return KEY_V1;
        }

        bool operator < (const struct key_struct& b) const {
            if (dir_id < b.dir_id) return true;
            if (dir_id > b.dir_id) return false;
            //  dir_id == b.dir_id
            if (obj_id < b.obj_id) return true;
            if (obj_id > b.obj_id) return false;
            // obj_id == b.obj_id
            if (this->offset(KEY_VGUESS) < b.offset(KEY_VGUESS)) return true;
            if (this->offset(KEY_VGUESS) > b.offset(KEY_VGUESS)) return false;
            // should reach here only when keys are equal
            assert2 ("key types should be equal", this->type(KEY_VGUESS) == b.type(KEY_VGUESS));
            if (this->type(KEY_VGUESS) < b.type(KEY_VGUESS)) return true;
            return false;
        }
        bool operator > (const struct key_struct& b) const {
            if (dir_id > b.dir_id) return true;
            if (dir_id < b.dir_id) return false;
            //  dir_id == b.dir_id
            if (obj_id > b.obj_id) return true;
            if (obj_id < b.obj_id) return false;
            // obj_id == b.obj_id
            if (this->offset(KEY_VGUESS) > b.offset(KEY_VGUESS)) return true;
            if (this->offset(KEY_VGUESS) < b.offset(KEY_VGUESS)) return false;
            // should reach here only when keys are equal
            assert2 ("key types should be equal", this->type(KEY_VGUESS) == b.type(KEY_VGUESS));
            if (this->type(KEY_VGUESS) > b.type(KEY_VGUESS)) return true;
            return false;
        }
        bool operator == (const struct key_struct& b) const {
            return dir_id == b.dir_id && obj_id == b.obj_id && v0.offset == b.v0.offset
                && v0.type == b.v0.type;
        }
        bool operator != (const struct key_struct& b) const {
            return dir_id != b.dir_id || obj_id != b.obj_id || v0.offset != b.v0.offset
                || v0.type != b.v0.type;
        }
        bool operator >= (const struct key_struct& b) const { return (*this > b) || (*this == b); }
        bool operator <= (const struct key_struct& b) const { return (*this < b) || (*this == b); }

        uint32_t offset_v0() const { return v0.offset; }
        uint64_t offset_v1() const { return v1.offset; }
        uint64_t offset(int key_version) const {
            if (KEY_VGUESS == key_version) key_version = this->guessVersion();
            switch (key_version) {
            case KEY_V0: return this->offset_v0(); break;
            case KEY_V1: return this->offset_v1(); break;
            default: assert2 ("key_t::offset(): wrong key type", false);
            }
        }
        uint32_t type_v0() const { return v0.type; }
        uint32_t type_v1() const { return v1.type; }
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
        void dump(int key_version, std::ostream &stream, bool need_endl = false) const {
            switch (key_version) {
            case KEY_V0: this->dump_v0(stream, need_endl); break;
            case KEY_V1: this->dump_v1(stream, need_endl); break;
            default: assert2 ("key_t::dump(): wrong key type", false);
            }
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
        uint32_t type(int key_version) const {
            if (KEY_VGUESS == key_version) key_version = this->guessVersion();
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
        bool sameObjectAs(const struct key_struct &another) const {
            return ((this->dir_id == another.dir_id)
                    && (this->obj_id == another.obj_id));
        }
    } __attribute__ ((__packed__));
    typedef struct key_struct key_t;

    struct tree_ptr {
        uint32_t block;
        uint16_t size;
        uint16_t reserved;
    } __attribute__ ((__packed__));

    struct de_header {
        uint32_t hash_gen;  //< hash [30:7] and generation [0:6]
        uint32_t dir_id;    //< dir_id \___ points to
        uint32_t obj_id;    //< obj_id /      object
        uint16_t location;  //< offset of name (inside item)
        uint16_t state;     //< 4 means visible
    } __attribute__ ((__packed__));

    struct item_header {
        key_t key;
        uint16_t count;
        uint16_t length;
        uint16_t offset;
        uint16_t version;
        uint32_t type() const { return this->key.type(this->version); }
    } __attribute__ ((__packed__));

    const key_t &key(uint32_t index) const {
        const key_t *kp = reinterpret_cast<const key_t *>(&buf[0] + 24 + 16*index);
        const key_t &kpr = kp[0];
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
    const struct de_header &dirHeader(const struct item_header &ih, uint32_t index) const {
        const struct de_header *dehp =
            reinterpret_cast<const struct de_header*>(&buf[0] + ih.offset + 16 * index);
        const struct de_header &dehpr = dehp[0];
        return dehpr;
    }

    /// retrieve object name
    std::string dirEntryName(const struct item_header &ih, uint32_t index) const {
        std::string name;
        const struct de_header &deh = this->dirHeader(ih, index);
        uint32_t end_pos = ih.offset;
        if (index > 0)
            end_pos += this->dirHeader(ih, index - 1).location;
        else
            end_pos += ih.length;
        uint32_t k = deh.location + ih.offset;
        while (k < end_pos && buf[k] != 0)
            name += buf[k++];
        return name;
    };

    const uint32_t &indirectItemRef(const struct item_header &ih, uint32_t idx) const {
        const uint32_t *ci = reinterpret_cast<uint32_t const *>(&buf[0] + ih.offset + 4*idx);
        const uint32_t &ref = ci[0];
        return ref;
    }
    void setIndirectItemRef(const struct item_header &ih, uint32_t idx, uint32_t value) {
        uint32_t *ci = reinterpret_cast<uint32_t *>(&buf[0] + ih.offset + 4*idx);
        ci[0] = value;
        this->dirty = true;
    }

    static const key_t zero_key;
    static const key_t largest_key;
};

class FsJournal {
public:
    FsJournal(int fd_, FsSuperblock *sb);
    ~FsJournal();
    Block* readBlock(uint32_t block_idx, bool caching = true);
    void readBlock(Block &block_obj, uint32_t block_idx);
    int writeBlock(Block *block_obj, bool factor_into_trasaction = true);
    void releaseBlock(Block *block_obj, bool factor_into_trasaction = true);
    void moveRawBlock(uint32_t from, uint32_t to, bool factor_into_trasaction = true);
    void beginTransaction();
    int commitTransaction();
    int flushTransactionCache();
    uint32_t estimateTransactionSize();
    void setCacheSize(uint32_t mib) { this->max_cache_size = mib * BLOCKS_IN_ONE_MB; }
    uint32_t cacheSize() const { return this->max_cache_size / BLOCKS_IN_ONE_MB; }

private:
    struct cache_entry {
        Block *block_obj;
        int priority;
    };
    struct {
        uint32_t last_flush_id;
        uint32_t unflushed_offset;
        uint32_t mount_id;
    } __attribute__ ((__packed__)) journal_header;

    bool use_journaling;
    bool flag_transaction_max_size_exceeded;
    int fd;
    FsSuperblock *sb;
    std::map<uint32_t, cache_entry> block_cache;
    int64_t cache_hits;
    int64_t cache_misses;
    uint32_t max_cache_size;    //< soft size border for read cache
    uint32_t max_batch_size;    //< maximum transaction batch size
    struct {
        std::set<Block *> blocks;
        bool running;
        bool batch_running;
    } transaction;
    movemap_t raw_moves;

    bool blockInCache(uint32_t block_idx) { return this->block_cache.count(block_idx) > 0; }
    void pushToCache(Block *block_obj, int priority = CACHE_PRIORITY_NORMAL);
    void deleteFromCache(uint32_t block_idx);
    void touchCacheEntry(uint32_t block_idx);
    void eraseOldestCacheEntry();
    int writeJournalEntry();
    int doCommitTransaction();
    void flushRawMoves();
};

class FsBitmap {
public:
    typedef struct {
        uint32_t start;
        uint32_t len;
    } extent_t;
    struct ag_entry {
        std::vector<extent_t> list;
        bool need_update;
        uint32_t used_blocks;
        ag_entry() {
            need_update = true;
        }
        extent_t & operator [] (uint32_t k) { return this->list[k]; }
        const extent_t & operator [] (uint32_t k) const { return this->list[k]; }
        void push_back(const extent_t &ex) { this->list.push_back(ex); }
        void clear() { this->list.clear(); }
        std::vector<extent_t>::size_type size() const { return this->list.size(); }
    };
    typedef struct ag_entry ag_entry;

    FsBitmap(FsJournal *journal, const FsSuperblock *sb);
    ~FsBitmap();
    bool blockUsed(uint32_t block_idx) const;

    /// checks if block is in reserved area, such as journal, sb, bitmap of first 64kiB
    bool blockReserved(uint32_t block_idx) const;

    void markBlockUsed(uint32_t block_idx);
    void markBlockFree(uint32_t block_idx);
    void markBlock(uint32_t block_idx, bool used);
    void writeChangedBitmapBlocks();
    void updateAGFreeExtents();
    void rescanAGForFreeExtents(uint32_t ag);
    /// returns count of allocation groups
    uint32_t AGCount() const { return this->ag_free_extents.size(); }
    uint32_t AGOfBlock(uint32_t block_idx) const { return block_idx / this->ag_size; }
    uint32_t AGSize(uint32_t ag) const;
    uint32_t AGBegin(uint32_t ag) const;
    uint32_t AGEnd(uint32_t ag) const;
    uint32_t AGExtentCount(uint32_t ag) const { return this->ag_free_extents[ag].size(); }
    uint32_t AGUsedBlockCount(uint32_t ag) const { return this->ag_free_extents[ag].used_blocks; }
    uint32_t AGFreeBlockCount(uint32_t ag) const;
    /// sets size of each allocation group
    void setAGSize(uint32_t size);

    /// allocate free blocks, continuous
    ///
    /// \param  ag[in,out]          hint (for input), next hint (for output)
    /// \param  required_size[in]   required size of extent
    /// \param  blocks[out]         allocated blocks
    /// \return RFSD_OK if allocation was successful, RFSD_FAIL otherwise
    int allocateFreeExtent(uint32_t &ag, uint32_t required_size, std::vector<uint32_t> &blocks,
                           uint32_t forbidden_ag = -1);

private:
    FsJournal *journal;
    const FsSuperblock *sb;
    std::vector<Block> bitmap_blocks;
    uint32_t ag_size;       //< size of each allocation group, in blocks (last AG may be smaller)
    std::vector<ag_entry> ag_free_extents; //< list of free extents in each AG

    uint32_t sizeInBlocks() const { return this->sb->s_block_count; }

    /// \return count of reserved blocks in specified AG
    uint32_t reservedBlockCount(uint32_t ag) const;

    /// \return count of reserved blocks in [from, to] segment
    uint32_t reservedBlockCount(uint32_t from, uint32_t to) const;

    /// \return true if \param block_idx points to bitmap
    bool blockIsBitmap(uint32_t block_idx) const;

    /// \return true if \param block_idx points journal
    bool blockIsJournal(uint32_t block_idx) const;

    /// \return true if \param block_idx points to first 64k
    bool blockIsFirst64k(uint32_t block_idx) const;

    /// \return true if \param block_idx points to superblock
    bool blockIsSuperblock(uint32_t block_idx) const;
};

class ReiserFs {
public:
    typedef struct {
        uint32_t type;
        uint32_t idx;
    } tree_element;
    struct leaf_index_entry {
        leaf_index_entry() { changed = false; };
        bool changed;
        std::set<uint32_t> leaves;
    };

    ReiserFs();
    ~ReiserFs();
    int open(const std::string &name, bool o_sync = true);
    void close();
    uint32_t moveBlocks(movemap_t &movemap);
    void dumpSuperblock();
    void useDataJournaling(bool use);
    uint32_t freeBlockCount() const;
    void setCacheSize(uint32_t mib) { this->cache_size = mib; }
    uint32_t cacheSize() const { return this->cache_size; }

    // proxies for FsJournal methods
    Block* readBlock(uint32_t block) const;
    void releaseBlock(Block *block) const;

    void printFirstFreeBlock();
    uint32_t findFreeBlockBefore(uint32_t block_idx) const;
    uint32_t findFreeBlockAfter(uint32_t block_idx) const;
    bool blockUsed(uint32_t block_idx) const { return this->bitmap->blockUsed(block_idx); }
    uint32_t sizeInBlocks() const { return this->sb.s_block_count; }
    void looseWalkTree();
    void enumerateTree(std::vector<tree_element> &tree) const;
    void enumerateInternalNodes(std::vector<tree_element> &tree) const;

    Block::key_t findObject(const std::string &fname) const;
    Block::key_t findObjectAt(const std::string &fname, const Block::key_t &at) const;
    uint32_t getStringHashR5(const std::string &s) const;

    /// walk tree, collecting leaves
    ///
    /// traverse tree from \p start_key, stop walking after getting \p soft_threshold blocks
    /// (both leaves and unformatted). Returns vector of pointers to leaves in \p leaves and
    /// key of last element in last leaf in \p last_key. \p leaves vector will be cleared.
    /// \param  start_key[in]
    /// \param  soft_threshold[in]
    /// \param  leaves[out]
    /// \param  last_key[out]
    void enumerateLeaves(const Block::key_t &start_key, int soft_threshold,
                         std::vector<uint32_t> &leaves, Block::key_t &last_key) const;

    /// get blocks relevant to object
    ///
    /// \param start_key[in]    object to be dumped
    /// \param start_offset[in] count of blocks to skip in first indirect item of first leaf
    /// \param next_key[out]    receives next object key (dir_id, obj_id pair)
    /// \param next_offset[out] receives saved position to restart easing
    /// \param blocks[out]      receives vector of blocks
    /// \param limit            hard limits on block count
    void getIndirectBlocksOfObject(const Block::key_t &start_key, uint32_t start_offset,
                                   Block::key_t &next_key, uint32_t &next_offset,
                                   blocklist_t &blocks, uint32_t limit) const;

    /// move movable blocks of range [ @from, @to] (borders included) below @to
    void cleanupRegionMoveDataDown(uint32_t from, uint32_t to);

    int squeezeDataBlocksInAG(uint32_t ag);
    /// print movemap contents to stdout
    void dumpMovemap(const movemap_t &movemap) const;

    /// moves all movable blocks outside AG
    int sweepOutAG(uint32_t ag);

    /// marks AG # \param ag as unavailable for sweeping
    void sealAG(uint32_t ag);

    /// figures out if AG # \param ag available for sweeping or not
    bool AGSealed(uint32_t ag);

    /// \return true if \param block_idx points to reserved block
    bool blockReserved(uint32_t block_idx) const { return this->bitmap->blockReserved(block_idx); }

    void setupInterruptSignalHandler();

    static bool userAskedForTermination();

    FsBitmap *bitmap;

private:
    FsJournal *journal;
    FsSuperblock sb;
    std::string fname;
    int fd;
    bool closed;
    bool use_data_journaling;
    std::string err_string;
    uint32_t blocks_moved_formatted;    //< counter used for moveMultipleBlocks
    uint32_t blocks_moved_unformatted;  //< counter used for moveMultipleBlocks
    std::vector<leaf_index_entry> leaf_index;
    uint32_t leaf_index_granularity;    //< size of each basket for leaf index
    static int interrupt_state;
    uint32_t cache_size;
    std::vector<bool> sealed_ags;

    int readSuperblock();
    int validateSuperblock();
    void writeSuperblock();
    bool movemapConsistent(const movemap_t &movemap);
    void collectLeafNodeIndices(uint32_t block_idx, std::vector<uint32_t> &lni);
    void recursivelyMoveInternalNodes(uint32_t block_idx, movemap_t &movemap,
        uint32_t target_level);
    /// traverses tree, moves unformatted blocks
    void recursivelyMoveUnformatted(uint32_t block_idx, movemap_t &movemap);
    uint32_t estimateTreeHeight();
    void recursivelyEnumerateNodes(uint32_t block_idx, std::vector<ReiserFs::tree_element> &tree,
                                   bool only_internal_nodes) const;
    /// worker method for leaves enumeration
    ///
    /// \param block_idx[in]        target block
    /// \param start_key[in]        do not process items with keys lower that \p start_key
    /// \param soft_threshold[in/out] stop walking tree after so many blocks collected
    /// \param left[in]             left border hint
    /// \param right[in]            right border hint
    /// \param leaves[out]          output vector which receives leaf list
    /// \param last_key[out]        receives key of last processed item
    void recursivelyEnumerateLeaves(uint32_t block_idx, const Block::key_t &start_key,
                                    int &soft_threshold, Block::key_t left, Block::key_t right,
                                    std::vector<uint32_t> &leaves, Block::key_t &last_key) const;
    /// creates list of leaves that point to blocks in specific basket
    ///
    /// \return RFSD_OK on success and RFSD_FAIL on failure
    int createLeafIndex();
    /// removes obsolete entries after block movement
    void updateLeafIndex();

    /// move unformatted blocks of specific leaf
    ///
    /// permorm move of unformatted data blocks provided in @movemap from leaf specified by
    /// @block_idx. Takes @key_list as mandatory hint
    void leafContentMoveUnformatted(uint32_t block_idx, movemap_t &movemap,
                                    const std::set<Block::key_t> &key_list, bool all_keys = false);
    void getLeavesForBlockRange(std::vector<uint32_t> &leaves, uint32_t from, uint32_t to);
    void getLeavesForMovemap(std::vector<uint32_t> &leaves, const movemap_t &movemap);

    /// inner worker function for getLeavesOfObject
    bool recursivelyGetBlocksOfObject(const uint32_t leaf_idx, const Block::key_t &start_key,
                                      const uint32_t object_type, const Block::key_t left,
                                      const Block::key_t right, uint32_t &start_offset,
                                      blocklist_t &blocks, Block::key_t &next_key,
                                      uint32_t &next_offset, uint32_t &limit) const;

    static void interruptSignalHandler(int arg);
};

int readBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size);
int writeBufAt(int fd, uint32_t block_idx, void *buf, uint32_t size);

class Defrag {
public:
    Defrag (ReiserFs &fs);

    /// performs defragmentation by sorting blocks in tree order
    ///
    /// \param batch_size[in]   controls batch granularity
    /// \return RFSD_OK on success, RFSD_FAIL otherwise
    int DefragTreeThrough(uint32_t batch_size = 16000);

    /// performs defragmentation path
    ///
    /// \param batch_size[in]   controls batch granularity
    /// \return RFSD_OK on success, RFSD_FAIL otherwise
    int DefragPath(uint32_t batch_size = 16000);

    /// performs incremental defragmentation
    ///
    /// \param batch_size[in]               controls batch granularity
    /// \param use_previous_estimation[in]  use previous estimation instead of doing it again
    ///                                     have no effect on first use
    /// \return RFSD_OK on success, RFSD_FAIL otherwise
    int DefragIncremental(uint32_t batch_size = 8000, bool use_previous_estimation = true);

    /// returns how many failed or incomplete defragmentation tasks there was last time
    uint32_t lastDefragImperfectCount();

    /// squeezes all AGs having more than \param threshold free extents
    int squeezeAllAGsWithThreshold(uint32_t threshold);

    /// moves files to beginning of the fs
    ///
    /// \param obj[in]          list of keys with (dir_id,obj_id) denoting a file to move
    /// \return RFSD_OK on success, RFSD_FAIL otherwise
    int moveObjectsUp(const std::vector<Block::key_t> &objs);

    /// prevent object in \param objs from moving
    void sealObjects(const std::vector<Block::key_t> &objs);

private:
    ReiserFs &fs;
    uint32_t desired_extent_length;
    uint32_t previous_obj_count;
    std::set<Block::key_t> sealed_objs;

    struct defrag_statistics_struct {
        uint32_t success_count;
        uint32_t partial_success_count;
        uint32_t failure_count;
        uint32_t total_count;
        void reset() {
            total_count = 0;
            success_count = 0;
            partial_success_count = 0;
            failure_count = 0;
        }
    } defrag_statistics;

    uint32_t nextTargetBlock(uint32_t previous);
    void createMovemapFromListOfLeaves(movemap_t &movemap, const std::vector<uint32_t> &leaves,
                                       uint32_t &free_idx);
    /// prepare movement map that defragments file specified by \param blocks
    ///
    /// \param  blocks[in]      block list
    /// \param  movemap[out]    resulting movement map
    /// \return RFSD_OK on partial success and RFSD_FAIL if all attempts failed
    int prepareDefragTask(std::vector<uint32_t> &blocks, movemap_t &movemap);
    uint32_t getDesiredExtentLengths(const std::vector<FsBitmap::extent_t> &extents,
                                     std::vector<uint32_t> &lengths, uint32_t target_length);
    void convertBlocksToExtents(const std::vector<uint32_t> &blocks,
                                std::vector<FsBitmap::extent_t> &extents);
    /// filters out sparse blocks from \param blocks by eliminating all zeros
    void filterOutSparseBlocks(std::vector<uint32_t> &blocks);

    /// merges \param dest and \param src to \param dest
    ///
    /// \param  dest[in,out]    first source and destination
    /// \param  src[in]         second source
    /// \return RFSD_OK if merge successful, RFSD_FAIL if there was some overlappings
    int mergeMovemap(movemap_t &dest, const movemap_t &src);

    /// frees at least one AG by moving it contents out
    ///
    /// \return RFSD_OK on success, RFSD_FAIL otherwise
    int freeOneAG();

    /// prints defrag statistics to stdout
    void showDefragStatistics();

    /// determine if \param k refers sealed fs object
    bool objectIsSealed(const Block::key_t &k) const;
};

class Progress {
public:
    Progress(uint32_t mv = 100);
    ~Progress();

    void setMaxValue(uint32_t value) { this->max_value = std::max(1u, value); }
    void setName(const std::string &nm) { this->name = nm; this->show_name = true; }
    void update(uint32_t value);
    void inc(uint32_t delta = 1);
    void show100();
    void abort();

    void showRawValues(bool v) { this->show_raw_values = v; }
    void showPercentage(bool v) { this->show_percentage = v; }
    void showProgressBar(bool v) { this->show_progress_bar = v; }
    void showName(bool v) { this->show_name = v; }
    void enableUnknownMode(bool v, uint32_t interval) {
        this->unknown_mode = v;
        if (v) this->unknown_interval = interval;
    }

private:
    uint32_t max_value;
    uint32_t prev_ppt;
    uint32_t prev_value;
    bool show_raw_values;
    bool show_percentage;
    bool show_progress_bar;
    bool show_name;
    bool unknown_mode;
    std::string name;
    time_t start_time;
    uint32_t unknown_interval;

    void displayKnown(uint32_t value);
    void displayUnknown(uint32_t value);
    /// determine terminal width
    uint32_t getWidth();
};
