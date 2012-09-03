project (reiserfs-toys)

cmake_minimum_required (VERSION 2.6)

add_library (mrfsu STATIC
	../reiserfs.cpp
	../journal.cpp
	../bitmap.cpp
	../block.cpp
)

add_executable (tuxonizer tuxonizer.cpp)
target_link_libraries(tuxonizer mrfsu)

add_executable (moveback moveback.cpp)
target_link_libraries(moveback mrfsu)
