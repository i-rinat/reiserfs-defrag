#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("../image/reiserfs.image");
    fs.printFirstFreeBlock();
    fs.moveBlock(250000, 8211);
    fs.close();

    return 0;
}
