import subprocess
import os

def configureAndMake(cppc, opts, buildtype):
    cxx = "CXX=" + cppc;
    dirname = cppc + '_' + ''.join(opts) + '_' + buildtype
    params = "CXXFLAGS='-Wall -Werror {}'".format(' '.join(opts));
    try:
        os.mkdir(dirname);
    except OSError:
        pass
    cmd = "( cd {}; {} {} cmake -DCMAKE_BUILD_TYPE={} ../..; make -j2 )".format( \
        dirname, cxx, params, buildtype)
    ret = subprocess.call(cmd, shell=True);
    if (0 != ret):
        return "[failed] {}\n".format(dirname)
    else:
        return "[  ok  ] {}\n".format(dirname)

f = open("buildlog", "wt")
for cppc in ('clang++', 'g++'):
    for opt1 in ('-O0', '-O1', '-O2', '-O3'):
        for opt2 in ('', '-m32'):
            for buildtype in ('Debug', 'Release'):
                ret = configureAndMake(cppc, (opt1, opt2), buildtype)
                f.write(ret)
                f.flush()

f.close()
