Why it is done the way it is done
=================================

How keys are compared
---------------------
Keys in reiserfs have 128-bit length both in v3.5 and v3.6. Both versions have
the same first and second 32-bit part, but differ in third and forth. As there
is no version stored inside key itself, one must somehow figure out what version
he has. Fortunatelly key version can be determined by examining _type_ field of
key assuming key has v3.6. All types of keys v3.5 will have `0xF` or `0x0` type.
Stat keys of v3.6 with have `0x0` type too, but that's ok, they don't differ from
v3.5 anyway. So keys are first converted to same version and then compared,
field by field. _dir_id_, _obj_id_, _offset_, _type_, in that order.


What is leaf index?
-------------------
Filesystem information stored in the tree. Metadata, such as file names, access
permissions and times, and short files themselves stored in that tree. Bigger files
stored outside of the tree, but pointers to where data actually is are in tree too.
Whole structure of filesystem was designed to solve problem of storing and accessing
data. The task defragmentation utility has to go opposite. It needs to move data
and update corresponding pointers in the tree. But there is no way to figure out
where those pointers are just from file data.

That issue addressed by leaf index. All filesystem divided to buckets of say 2000 blocks.
and for each such bucket list of leaves that have at least one pointer to that bucket.
If one need to figure out which leaf contains block, he first determines bucket number
and then gets list of leaves that can contain pointer to that block.

Leaf index is in-memory data structure and first constructed on filesystem opening.
It is unintrusive operation and can be interrupted with no consequences. Then, while
utility operates, it maintains leaf index, to ensure it always contains relevant
information.

How tree-through defrag works
-----------------------------
Tree-through defrag packs the whole tree in key order. First all internal nodes, then
leaves and file contents. There is the method exists, which traverses tree starting
from _start-key_ and returns list of leaves. That method accepts soft limit on block
count, after reaching of which traversing stops. Here is high level algorithm:

   1. Get list of leaves strating from _start_key_
   2. _old_free_idx_ := _free_idx_
   3. Construct move map, advancing _free_idx_
   4. If map empty, go to step 10
   5. Free area \[_old_free_idx_, _free_idx_-1\], moving blocks towards the end
   6. _free_idx_ := _old_free_idx_
   7. Get list of leaves starting from _start_key_
   8. Construct move map, advancing _free_idx_
   9. Move blocks
   10. _start_key_ := _last_used_key_

Each iteration constructs move map twice, once for determine area to be freed, and
once again to actually move blocks. That sounds wierd first, but blocks to be moved
could be in freed area, so one need to determine their position again.


How incremental defrag works
----------------------------
It traverses tree, one file at a time. Then determines, if each 2048-block slice of file
is in _ideal_ order. If no, searches for free extent of such size and moves slice
there. If there is no any free extent of requested size, cleaning procedure starts.
Cleaning procedure selects AG based on its free space fragmentation and some random
number, and moves all its contents away from this AG. Despite lack of sophistication,
result is fine usually. Free space allocator tries to allocate continuous chunks so
such sweeping usually not increasing fragmentation more. To address possible harm,
incremental defragmentation is done in multiple passes (3 by default).

Allocation groups (AG)
----------------------
All filesystem divided to 128 MiB chunks. They are used to allocate blocks. Each such
chunk have corresponding in-memory structure which contains list of free extents.
Reiserfs itself have one bitmap block for every 128 MiB, so there is no way to allocate
extent longer than 128 MiB - 4 KiB. There is possibility to select larger AG, 256 MiB and
even 512 MiB, but that's not tested.


Squeeze operation
-----------------
While defragmentation operates, AG's free space can become really fragmented. To address
that, squeeze operation exists. Its result is placing all occupied blocks at the beginning
of AG. I.e. if before AG contained something like `###1##2#####3##4` (where # -- free
blocks) after squeezing it will contain `1234############`. Data blocks' order's preserved.
Contrary to AG sweeping operation, block of AG remains in that AG, but squeezing run time
at least twice as long as sweeping run time.

Ideal block order
-----------------
Tree-through defrag uses something called _ideal_ order. Here what it is. For large files
reiserfs uses so called indirect items. They are similar to indirect blocks in ext2. They
contain links to blocks where actual data stored. 4 KiB leaf block can contain 1012 pointers,
thus can address at most 4048 KiB. Authors of reiserfs implemented driver in such way, it
places leaf nodes interleaved with data blocks:

`       leaf node -->  data blocks --> leaf node --> data blocks --> ...`

_reiserfs-defrag_ uses similar sequence. But leaves included in this sequence only
if relevant indirect item is first in leaf. That is because one leaf can contain may indirect
items and refer to different files, but can not belong to two or more sequences.

File names packing in directory items
-------------------------------------
Available documentation states names are zero-terminated. But in practice, they are zero-padded.
For example, 16-byte filenames stored without zeros between them. Anyway, directory items have
enough information to determine, where name ends.

Great source of knowledge
-------------------------
I used following reference to learn how reiserfs lays on disk:

[The structure of the Reiser file system by Florian Buchholz](http://homes.cerias.purdue.edu/~florian/reiser/reiserfs.php)
