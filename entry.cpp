#include "reiserfs.hpp"
#include <getopt.h>
#include <stdio.h>
#include <iostream>

const int DEFRAG_TYPE_INCREMENTAL = 0;
const int DEFRAG_TYPE_TREETHROUGH = 1;

static const char *opt_string = "t:h";
static const struct option long_opts[] = {
    { "help",   no_argument,        NULL, 'h' },
    { "type",   required_argument,  NULL, 't' },
    { 0, 0, 0, 0}
};

struct params_struct {
    int defrag_type;
} params;

void
display_usage()
{
    printf("Usage: reiserfs-defrag [options] <reiserfs partition>\n");
    printf("\n");
}

void default_params()
{
    params.defrag_type = DEFRAG_TYPE_INCREMENTAL;
}

int
main (int argc, char *argv[])
{
    int opt, long_index;

    opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    while (-1 != opt) {
        switch (opt) {
        case 't':
            if (std::string("incremental") == optarg || std::string("inc") == optarg)
            {
                params.defrag_type = DEFRAG_TYPE_INCREMENTAL;
            } else if (std::string("treethrough") == optarg ||
                std::string("tree-through") == optarg || std::string("tree") == optarg)
            {
                params.defrag_type = DEFRAG_TYPE_TREETHROUGH;
            } else {
                std::cout << "wrong defrag algorithm: " << optarg << std::endl;
                return 2;
            }
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
        if (RFSD_OK != fs.open(argv[optind], false))
            return 1;
    } else {
        if (RFSD_OK != fs.open("../image/reiserfs.image", false))
            return 1;
    }
    fs.useDataJournaling(false);

    Defrag defrag(fs);

    switch (params.defrag_type) {
    case DEFRAG_TYPE_INCREMENTAL:
        std::cout << "defrag type: incremental" << std::endl;
        defrag.experimental_v2();
        break;
    case DEFRAG_TYPE_TREETHROUGH:
        std::cout << "defrag type: treethrough" << std::endl;
        defrag.treeThroughDefrag(8000);
        break;
    }

    fs.close();
    return 0;
}
