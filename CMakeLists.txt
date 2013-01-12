project (reiserfs-toys)

cmake_minimum_required (VERSION 2.6)

add_executable (reiserfs-defrag
	defrag.cpp
	reiserfs.cpp
	journal.cpp
	bitmap.cpp
	block.cpp
	progress.cpp
	entry.cpp
)

target_link_libraries(reiserfs-defrag
	rt
)

install(TARGETS reiserfs-defrag DESTINATION sbin)
install(FILES doc/reiserfs-defrag.8 DESTINATION "share/man/man8")
