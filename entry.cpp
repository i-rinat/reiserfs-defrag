#include "reiserfs.hpp"
#include <getopt.h>
#include <stdio.h>

static const char *opt_string = "t:h";
static const struct option long_opts[] = {
    { "help",   no_argument,        NULL, 'h' },
    { "type",   required_argument,  NULL, 't' },
    { 0, 0, 0, 0}
};

void
display_usage()
{
    printf("Usage: reiserfs-defrag [options] <reiserfs partition>\n");
    printf("\n");
}

int
main (int argc, char *argv[])
{
    int opt, long_index;

    opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    while (-1 != opt) {
        switch (opt) {
        case 't':

            break;
        case 'h':
            display_usage();
            return 0;
            break;
        }

        opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    }

    ReiserFs fs;

    if (argc - optind >= 1) {
        if (RFSD_OK != fs.open(argv[argc - optind], false))
            return 1;
    } else {
        if (RFSD_OK != fs.open("../image/reiserfs.image", false))
            return 1;
    }
    fs.useDataJournaling(false);

    Defrag defrag(fs);
    defrag.experimental_v2();

    fs.close();
    return 0;
}
