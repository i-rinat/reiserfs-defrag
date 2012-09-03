project (reiserfs-toys)

cmake_minimum_required (VERSION 2.6)

add_executable (tuxonizer
	tuxonizer.cpp
	../reiserfs.cpp
	../journal.cpp
	../bitmap.cpp
	../block.cpp
)

add_executable (moveback
	moveback.cpp
	../reiserfs.cpp
	../journal.cpp
	../bitmap.cpp
	../block.cpp
)
