About
=====
`reiserfs-defrag` is a software that will (try to) rearrange data on your **reiserfs**
partition to make it less fragmented. Offline. You need to unmount fs first.

Status
======
**Experimental. Do not use it on sensitive data.**

As for now (tag:*tree-through-v2*) it journals meta-data while moving blocks.
That _should_ prevent data loss, but I did not verify that thoroughly. I think now it
has quadratic complexity to the partition size. I've sorted out quadratic cpu usage, but
it still have quadratic time due to move patterns. Now it also lacks
of wise algorithms and just packs data in tree order. (Here tree is the internal tree that
stores meta-data, it has no much common with directory tree). And that means almost every
block will be moved. Insanely slow.

Some results:

 1. 3 GiB, with 92162 files and 25050 dirs, 78% full, took 2 minutes to complete.
 2. 120 GiB, with 3820294 files and 328952 dirs, 80% full, took 350 minutes to complete.
 3. 120 GiB, with 54 files and 2 dirs, 78% full, took 54 minutes to complete.

All filesystems were 'just created'. 1) debian/non-free amd64 repo unpacked;
2) debian/main amd64 repo unpacked; 3) bunch of large files, each ~1.5 Gb in size.


Fragmentation
=============
You can start reading about it [here](http://en.wikipedia.org/wiki/Defragmentation).
In a couple of words, sometimes fragmentation of filesystem can decrease its performance.

There is one issue about reiserfs specifically. Due to internal structure, even
the only file on fs will be cut into fragments of size about four megabytes. Data
interleaved with so-called indirect blocks, that used by filesystem to map inode
to actual partition blocks. One such block is 4 kiB length so it doesn't introduce
hard-drive head movement and therefore have almost no impact on read speed
performance. But fragmentation measurements tools, such as `filefrag` from
`e2fsprogs` and based on it, report file as heavily fragmented although it is not.

Why
===
Many years I've heard opinions that none of Linux filesystems need defragmentation and
one should forget about it. That's not entirely true. Well, design of filesystem space
allocation prevents fragmentation, but can not eliminate it completely. As you write,
update, delete files, filesystem ages and performs worse.

For many years the only solution available was to back up data, recreate filesystem, and
copy data back. I want an alternative, in-place defragmentation.

I declare goals as:

 * (g1) program should try its best to prevent data loss;
 * (g2) program should use bounded amount of memory no matter of fs size;
 * (g3) program working time should no worse than linearly depend of fs size (O(n log n) fine too);
 * (g4) program should be able to do incremental (fast) defragmentation;
 * (g5) program should be able to move selected files to beginning of partition.

g1 and g2 are done. g3 should be considered done, as cpu usage should be almost linear.
I don't have clear algorithms for g4 and g5 for the moment.

Build and install
=================
You'll need cmake. Build:

* `mkdir build && cd build`
* `cmake -DCMAKE_BUILD_TYPE=Release ..`
* `make`

There is no install as no need of. Executable binary named `rfsd` appears in
build directory. It expects only one argument: path to device or fs image.

Copying
=======
GPLv2/GPLv3

Authors
=======
Rinat Ibragimov, ibragimovrinat-at-mail.ru, github.com/i-rinat/
