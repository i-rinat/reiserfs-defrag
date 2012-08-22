#include <stdint.h>
#include <string>
#include <map>

class FsJournal {
public:

};

class Block {
public:
};

class FormattedBlock : public Block {
public:
};

class UnformattedBlock : public Block {
public:
};

class FsBitmap {
public:
};

class FsSuperblock {
public:
};

class ReiserFs {
public:
    ReiserFs();
    ~ReiserFs();
    int open(std::string name);
    int close();
    int moveBlock(uint32_t from, uint32_t to);
    int moveMultipleBlocks(std::map<uint32_t, uint32_t> movemap);
    int beginTransaction();
    int commitTransaction();

    void dumpSuperblock();

private:
    FsBitmap bitmap;
    FsJournal journal;
    FsSuperblock sb;
    std::string fname;

    void readSuperblock(FsSuperblock &sb);
};
