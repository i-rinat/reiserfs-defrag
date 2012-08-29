#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("../image/reiserfs.image");
    fs.moveBlock(250000, 5678);
    fs.close();

    return 0;
}
