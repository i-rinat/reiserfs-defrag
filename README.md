About
=====
`reiserfs-defrag` is a software that will rearrange data on your **reiserfs**
partition to make it less fragmented. **Offline**. You must unmount fs first.

Status
======
Still considered experimental, although it will do its best to protect data from
corruption. I do run consistency check often in the development process, but
obviously that can't guarantee anything. Do not use it on sensitive data,
or backup them at least.

As for now (v0.2.1) it journals meta-data while moving blocks and runs some consistency
checks on metadata. That should prevent data loss even for partially damaged filesystem.
Anyway, running fsck before defragmentation is a good idea.

There are two available algorithms: _tree-through_ defragmentation and _incremental_ one.
First packs (with no spaces between) all files in a tree order, interleaving data blocks
with leaf and internal tree nodes. That will move almost all data blocks, and therefore
will be very slow. But in the end you'll find all files (and directories) in their
ideal order and free space consolidated at the end of the filesystem.

Second one called _incremental_ and selected by default. It makes several passes through
filesystem's internal tree and tries to defragment all that it considers fragmented.
Currently it check for every 2048-block length chuck to be in one piece. Those 2048 blocks
contains not only data blocks but metadata blocks too. As a result, file can be splitted
to several parts which will be distributed through the partition. That's not user usually
wants to see, but this is current limitation. 8 MiB chucks are large enough. If you
have a 15 ms, 100 MiB/sec disk, every seek takes same time as reading 1.5 MiB, so it's
about 16% performance penalty. That is acceptable in multitask environment.


Fragmentation
=============
You can start reading about it [here](http://en.wikipedia.org/wiki/Defragmentation).
In a couple of words, sometimes fragmentation of filesystem can decrease its performance.

There is one issue about reiserfs specifically. Due to internal structure, even
the only file on fs will be cut into fragments of size about four megabytes. Data
interleaved with so-called indirect blocks, used by filesystem to map inode
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

Goals g1 through g5 are now (v0.2.1) considered done.

Build and install
=================
(You'll need cmake.) To build:

* `mkdir build && cd build`
* `cmake -DCMAKE_BUILD_TYPE=Release ..`
* `make`

Note two dots in cmake parameters. They are point to directory with sources
and those dots are required.

To install, run as root:

* `make install`

That will place `reiserfs-defrag` binary to ${PREFIX}/sbin. It expects path to device or
filesystem image. There are some other options available, see usage (--help) for details.

Copying
=======
GPLv2/GPLv3

Authors
=======
Rinat Ibragimov, ibragimovrinat-at-mail.ru, https://github.com/i-rinat/
