#include "../reiserfs.hpp"
#include "tux_logo.c"

int
main (int argc, char *argv[])
{
    ReiserFs fs;

    fs.open("../image/reiserfs.image", false);

    movemap_t movemap;
    const uint32_t mapwidth = 111;
    const uint32_t y_shift = 300;
    uint32_t freeblock = 0;
    const uint32_t mw = (mapwidth < tux_logo.width) ? mapwidth : tux_logo.width;

    for (uint32_t iy = 0; iy < tux_logo.height; iy ++) {
        for (uint32_t ix = 0; ix < mw; ix ++) {
            uint32_t block_idx = (y_shift+iy)*mapwidth + ix;

            if (fs.blockUsed(block_idx)) {
                if (tux_logo.pixel_data[3*iy*tux_logo.width + 3*ix] > 0) {
                    freeblock = fs.findFreeBlockAfter(freeblock);
                    movemap[block_idx] = freeblock;
                }
            }
        }
    }
    fs.moveBlocks(movemap);

    fs.close();

    return 0;
}

