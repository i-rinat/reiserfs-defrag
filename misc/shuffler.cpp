/* shuffles blocks of fs, acting as fragmentator, opposite of defrag.
 * That may seem pointless, but it can create input data for defrag testing.
 */

#include "../reiserfs.hpp"
#include <stdlib.h>
#include <iostream>
#include <set>
#include <algorithm>

int
main (int argc, char *argv[])
{
    ReiserFs fs;
    fs.open("../image/reiserfs.image", false);
    srand(time(NULL));
    uint32_t fs_size = fs.sizeInBlocks();

    std::vector<uint32_t> free_blocks;
    std::vector<uint32_t> occupied_blocks;

    for (uint32_t k = 0; k < fs_size; k ++) {
        if (fs.blockReserved(k)) continue;
        if (fs.blockUsed(k)) occupied_blocks.push_back(k);
        else free_blocks.push_back(k);
    }
    std::random_shuffle (free_blocks.begin(), free_blocks.end());
    std::random_shuffle (occupied_blocks.begin(), occupied_blocks.end());

    ReiserFs::movemap_t movemap;

    for(std::vector<uint32_t>::const_iterator
            from = occupied_blocks.begin(), to = free_blocks.begin();
        (from != occupied_blocks.end()) && (to != free_blocks.end());
        ++from, ++to)
    {
        movemap[*from] = *to;
    }

    if (movemap.size() > 0) {
        fs.moveMultipleBlocks(movemap);
    }

    fs.close();

    return 0;
}

