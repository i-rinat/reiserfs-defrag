#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("fs.image");
    fs.beginTransaction();
    fs.moveBlock(1234, 5678);
    fs.commitTransaction();
    fs.close();

    return 0;
}
