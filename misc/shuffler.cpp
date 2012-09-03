/* shuffles blocks of fs, acting as fragmentator, opposite of defrag.
 * That may seem pointless, but it can create input data for defrag testing.
 */

#include "../reiserfs.hpp"
#include <stdlib.h>
#include <iostream>

uint32_t bigrandom() {
    return rand()<<15 | rand();
}

int
main (int argc, char *argv[])
{
    ReiserFs fs;
    fs.open("../image/reiserfs.image", false);
    srand(time(NULL));
    uint32_t fs_size = fs.sizeInBlocks();


    std::map<uint32_t, uint32_t> movemap;
    for (int k = 0; k < 5; k ++) {
        uint32_t bfrom = bigrandom()%fs_size;
        uint32_t bto = bigrandom()%fs_size;
        std::cout << bfrom << " " << bto << "!" << std::endl;
        while (bfrom < fs_size && !fs.blockUsed(bfrom)) bfrom ++;
        if (bfrom < fs_size) {
            bto = fs.findFreeBlockBefore(bto);
            if (bto != 0) {
                movemap[bfrom] = bto;
                std::cout << bfrom << " " << bto << ">" << std::endl;
            }
        }
    }

    if (movemap.size() > 0) {
        fs.moveMultipleBlocks(movemap);
    }


    fs.close();

    return 0;
}

