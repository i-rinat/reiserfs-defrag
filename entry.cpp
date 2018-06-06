/*
 *  reiserfs-defrag, offline defragmentation utility for reiserfs
 *  Copyright (C) 2012  Rinat Ibragimov
 *
 *  Licensed under terms of GPL version 3. See COPYING.GPLv3 for full text.
 */

#define _POSIX_C_SOURCE 199309L
#include "reiserfs.hpp"
#include <getopt.h>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

const int DEFRAG_TYPE_INCREMENTAL = 0;
const int DEFRAG_TYPE_TREETHROUGH = 1;
const int DEFRAG_TYPE_PATH = 2;
const int DEFRAG_TYPE_NONE = 3;

struct params_struct {
    int defrag_type;
    int pass_count;
    bool do_squeeze;
    int squeeze_threshold;
    bool journal_data;
    uint32_t cache_size;
    std::vector<std::string> firstfiles;
} params;

static const char *opt_string = "c:f:p:st:h";
static const struct option long_opts[] = {
    { "cache-size",         required_argument,  NULL, 'c' },
    { "file-list",          required_argument,  NULL, 'f' },
    { "help",               no_argument,        NULL, 'h' },
    { "squeeze",            no_argument,        NULL, 's' },
    { "squeeze-threshold",  required_argument,  NULL, 128 },
    { "type",               required_argument,  NULL, 't' },
    { "journal-data",       no_argument,        NULL, 129 },
    { 0, 0, 0, 0}
};

class user_asked_termination : public std::exception {};
class no_error : public std::exception {};

void
display_usage()
{
    printf("Usage: reiserfs-defrag [options] <reiserfs partition>\n"
    "\n"
    "  -c, --cache-size <size>      specify block cache size in MiB (200 by default)\n"
    "  -f, --file-list <filename>   move files from list in <filename> to\n"
    "                               beginning of the fs\n"
    "  -h, --help                   show usage (this screen)\n"
    "  --journal-data               journal data in unformatted blocks\n"
    "  -p <passcount>               incremental defrag pass count\n"
    "  -s, --squeeze                squeeze AGs\n"
    "  --squeeze-threshold <value>  squeeze AGs with more than 'value' gaps\n"
    "  -t, --type <name>            select defragmentation algorithm:\n"
    "                                 * tree/treethrough/tree-through\n"
    "                                 * inc/incremental (default)\n"
    "                                 * path\n"
    "                                 * none\n"
    );
}

void default_params()
{
    params.defrag_type = DEFRAG_TYPE_INCREMENTAL;
    params.pass_count = 3;
    params.do_squeeze = false;
    params.squeeze_threshold = 7;
    params.journal_data = false;
    params.cache_size = 200;
}

void fill_file_list_from_file(const std::string &fname)
{
    std::ifstream fp(fname.c_str());
    if (fp.is_open()) {
        std::string s;
        while (fp.good()) {
            std::getline(fp, s);
            if (0 != s.length())
                params.firstfiles.push_back(s);
        }
    }
    fp.close();
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
        case 'c':   // cache size
            {
                std::stringstream ss(optarg);
                ss >> params.cache_size;
            }
            break;
        case 'f':
            fill_file_list_from_file(optarg);
            break;
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
            } else if (std::string("path") == optarg) {
                params.defrag_type = DEFRAG_TYPE_PATH;
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
        case 129:   // journal-data
            params.journal_data = true;
            break;
        }

        opt = getopt_long(argc, argv, opt_string, long_opts, &long_index);
    }

    ReiserFs fs;
    Defrag defrag(fs);

    fs.setupInterruptSignalHandler();

    try {
        // set up fs parameters
        fs.useDataJournaling(params.journal_data);
        std::cout << "journaling mode: ";
        std::cout << (params.journal_data ? "data" : "metadata only") << std::endl;
        fs.setCacheSize(params.cache_size);
        std::cout << "max block cache size: " << fs.cacheSize() << " MiB" << std::endl;

        if (argc - optind >= 1) {
            if (RFSD_OK != fs.open(argv[optind], false)) {
                // User may ask to terminate while leaf index created
                if (ReiserFs::userAskedForTermination())
                    throw user_asked_termination();
                // otherwise there was some error, we should quit now
                return 1;
            }
        } else {
            display_usage();
            throw no_error();
        }

        // determine object key for every entry in params.firstfiles
        if (params.firstfiles.size() > 0) {
            std::set<Block::key_t> unique_objs;
            std::vector<Block::key_t> firstobjs;
            for (std::vector<std::string>::const_iterator it = params.firstfiles.begin();
                 it != params.firstfiles.end(); ++ it)
            {
                Block::key_t k = fs.findObject(*it);
                if (!k.sameObjectAs(Block::zero_key) && unique_objs.count(k) == 0) {
                    firstobjs.push_back(k);
                    unique_objs.insert(k);
                }
            }

            defrag.moveObjectsUp(firstobjs);
            defrag.sealObjects(firstobjs);
        }

        switch (params.defrag_type) {
        case DEFRAG_TYPE_INCREMENTAL:
            {
                std::cout << "defrag type: incremental" << std::endl;
                int pass = 0;
                while (pass < params.pass_count) {
                    std::cout << "pass " << pass+1 << " of " << params.pass_count << std::endl;
                    if (RFSD_FAIL == defrag.DefragIncremental(8000, true)) {
                        if (ReiserFs::userAskedForTermination()) {
                            throw user_asked_termination();
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
            defrag.DefragTreeThrough(8000);
            break;
        case DEFRAG_TYPE_PATH:
            std::cout << "defrag type: path" << std::endl;
            defrag.DefragPath(8000);
            break;
        case DEFRAG_TYPE_NONE:
            std::cout << "defrag type: none" << std::endl;
            break;
        }

        if (params.do_squeeze and not ReiserFs::userAskedForTermination()) {
            // do squeeze blocks
            if (RFSD_FAIL == defrag.squeezeAllAGsWithThreshold(params.squeeze_threshold)) {
                if (ReiserFs::userAskedForTermination()) {
                    throw user_asked_termination();
                } else {
                    std::cout << "can't squeeze" << std::endl;
                }
            }
        }
    } catch (user_asked_termination &uat) {
        std::cout << "user asked for termination" << std::endl;
    } catch (std::logic_error &le) {
        std::cout << std::endl << "something bad happened. All I know is:" << std::endl;
        std::cout << le.what() << std::endl;
        return 2;
    } catch (no_error &e) {
        // nothing
    }

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
