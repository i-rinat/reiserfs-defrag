#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("../image/reiserfs.image");
    // fs.moveBlock(1234, 5678);
    Block *block = fs.readBlock(18);
    fs.dumpBlock(block);
    fs.releaseBlock(block);
    fs.close();

    return 0;
}
