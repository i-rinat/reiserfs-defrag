cmake_minimum_required (VERSION 2.6...4.1)

project (reiserfs-defrag)

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

set(SBINDIR "${CMAKE_INSTALL_PREFIX}/sbin" CACHE PATH "installation path for binaries (sbin)")

install(TARGETS reiserfs-defrag DESTINATION ${SBINDIR})
install(FILES doc/reiserfs-defrag.8 DESTINATION "share/man/man8")
