#include "../reiserfs.hpp"
#include <stdio.h>

int
main (int argc, char *argv[])
{
    ReiserFs fs;
    fs.open("../image/reiserfs.image", false);

    ReiserFs::movemap_t movemap;
    uint32_t freeblock = fs.sizeInBlocks();

    uint32_t block_idx = 16 + 1 + 8192 + 1 + 1;

    while (block_idx < freeblock) {
        if (block_idx % BLOCKS_PER_BITMAP == 0) {
            block_idx ++; continue;
        }
        if (fs.blockUsed(block_idx)) {
            freeblock = fs.findFreeBlockBefore(freeblock);
            if (block_idx < freeblock)
                movemap[block_idx] = freeblock;
        }
        block_idx ++;
    }

    fs.moveMultipleBlocks(movemap);
    fs.close();
    return 0;
}

