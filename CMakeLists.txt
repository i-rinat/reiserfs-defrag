project (reiserfs-toys)

cmake_minimum_required (VERSION 2.6)

add_executable (rfsd
	defrag.cpp
	reiserfs.cpp
	journal.cpp
	bitmap.cpp
	block.cpp
	progress.cpp
	entry.cpp
)

target_link_libraries(rfsd
	rt
)
