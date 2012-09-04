#include "reiserfs.hpp"

int
main (int argc, char *argv[])
{
    ReiserFs fs;
    fs.open("../image/reiserfs.image");

    fs.close();
    return 0;
}
