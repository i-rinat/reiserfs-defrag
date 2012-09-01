#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("../image/reiserfs.image");
    //fs.looseWalkTree();
    fs.printFirstFreeBlock();

    // move last used to first free
    uint32_t bid1 = fs.findFreeBlockAfter(16+1+8192+1);
    uint32_t bid2 = fs.sizeInBlocks();
    while (bid2 > 0 && !fs.blockUsed(bid2)) bid2 --;

    fs.moveBlock(bid2, bid1);
    fs.close();

    return 0;
}
