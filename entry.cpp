#define _POSIX_C_SOURCE 199309L
#include "reiserfs.hpp"
#include <getopt.h>
#include <stdio.h>
#include <time.h>
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
    struct timespec start_time, stop_time;
    bool monotonic_clock_available = true;

    // get start time
    if (0 != clock_gettime(CLOCK_MONOTONIC, &start_time))
        monotonic_clock_available = false;

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
        defrag.incrementalDefrag();
        break;
    case DEFRAG_TYPE_TREETHROUGH:
        std::cout << "defrag type: treethrough" << std::endl;
        defrag.treeThroughDefrag(8000);
        break;
    }

    fs.close();

    // print elapsed time
    if (monotonic_clock_available) {
        clock_gettime(CLOCK_MONOTONIC, &stop_time);
        uint32_t elapsed_seconds = stop_time.tv_sec - start_time.tv_sec;
        std::cout << "elapsed time: " << elapsed_seconds << " s" << std::endl;
    }

    return 0;
}
