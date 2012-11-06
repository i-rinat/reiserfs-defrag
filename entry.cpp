#define _POSIX_C_SOURCE 199309L
#include "reiserfs.hpp"
#include <getopt.h>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include <sstream>

const int DEFRAG_TYPE_INCREMENTAL = 0;
const int DEFRAG_TYPE_TREETHROUGH = 1;
const int DEFRAG_TYPE_NONE = 2;

struct params_struct {
    int defrag_type;
    int pass_count;
    bool do_squeeze;
    int squeeze_threshold;
} params;

static const char *opt_string = "p:st:h";
static const struct option long_opts[] = {
    { "help",               no_argument,        NULL, 'h' },
    { "squeeze",            no_argument,        NULL, 's' },
    { "squeeze-threshold",  required_argument,  NULL, 128 },
    { "type",               required_argument,  NULL, 't' },
    { 0, 0, 0, 0}
};

void
display_usage()
{
    printf("Usage: reiserfs-defrag [options] <reiserfs partition>\n");
    printf("\n");
}

void default_params()
{
    params.defrag_type = DEFRAG_TYPE_INCREMENTAL;
    params.pass_count = 3;
    params.do_squeeze = false;
    params.squeeze_threshold = 7;
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

    // set up default parameter values
    default_params();

    opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    while (-1 != opt) {
        switch (opt) {
        case 'p':   // pass count
            {
                std::stringstream ss(optarg);
                if (!(ss >> params.pass_count)) params.pass_count = 1;
                if (params.pass_count < 1) params.pass_count = 1;
            }
            break;
        case 's':   // squeeze blocks
            params.do_squeeze = true;
            break;
        case 't':
            if (std::string("incremental") == optarg || std::string("inc") == optarg)
            {
                params.defrag_type = DEFRAG_TYPE_INCREMENTAL;
            } else if (std::string("treethrough") == optarg ||
                std::string("tree-through") == optarg || std::string("tree") == optarg)
            {
                params.defrag_type = DEFRAG_TYPE_TREETHROUGH;
            } else if (std::string("none") == optarg) {
                params.defrag_type = DEFRAG_TYPE_NONE;
            } else {
                std::cout << "wrong defrag algorithm: " << optarg << std::endl;
                return 2;
            }
            break;
        case 'h':
            display_usage();
            return 0;
            break;
        case 128:   // squeeze threshold
            {
                std::stringstream ss(optarg);
                ss >> params.squeeze_threshold;
                if (params.squeeze_threshold < 1) params.squeeze_threshold = 1;
                params.do_squeeze = true;
            }
            break;
        }

        opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    }

    ReiserFs fs;
    Defrag defrag(fs);

    fs.setupInterruptSignalHandler();
    if (argc - optind >= 1) {
        if (RFSD_OK != fs.open(argv[optind], false)) {
            // User may ask to terminate while leaf index created
            if (ReiserFs::userAskedForTermination())
                goto termination_point;
            // otherwise there was some error, we should quit now
            return 1;
        }
    } else {
        display_usage();
        goto termination_point;
    }

    fs.useDataJournaling(false);

    switch (params.defrag_type) {
    case DEFRAG_TYPE_INCREMENTAL:
        {
            std::cout << "defrag type: incremental" << std::endl;
            int pass = 0;
            while (pass < params.pass_count) {
                std::cout << "pass " << pass+1 << " of " << params.pass_count << std::endl;
                if (RFSD_FAIL == defrag.incrementalDefrag(8000, true)) {
                    if (ReiserFs::userAskedForTermination()) {
                        goto termination_point;
                    }
                    std::cout << "can't finish defragmentation. Perhaps free space is too low."
                        << std::endl;
                    break;
                }
                if (0 == defrag.lastDefragImperfectCount()) {
                    // we are done
                    std::cout << "defragmentation complete" << std::endl;
                    break;
                }
                pass ++;
            }
        }
        break;
    case DEFRAG_TYPE_TREETHROUGH:
        std::cout << "defrag type: treethrough" << std::endl;
        defrag.treeThroughDefrag(8000);
        break;
    case DEFRAG_TYPE_NONE:
        std::cout << "defrag type: none" << std::endl;
        break;
    }

    if (params.do_squeeze and not ReiserFs::userAskedForTermination()) {
        // do squeeze blocks
        if (RFSD_FAIL == defrag.squeezeAllAGsWithThreshold(params.squeeze_threshold)) {
            if (ReiserFs::userAskedForTermination()) {
                goto termination_point;
            } else {
                std::cout << "can't squeeze" << std::endl;
            }
        }
    }

termination_point:
    if (ReiserFs::userAskedForTermination())
        std::cout << "user asked for termination" << std::endl;

    fs.close();

    // print elapsed time
    if (monotonic_clock_available) {
        clock_gettime(CLOCK_MONOTONIC, &stop_time);
        uint32_t elapsed_seconds = stop_time.tv_sec - start_time.tv_sec;
        if (elapsed_seconds > 1) {
            std::cout << "elapsed time: " << elapsed_seconds << " s" << std::endl;
        }
    }

    return 0;
}
