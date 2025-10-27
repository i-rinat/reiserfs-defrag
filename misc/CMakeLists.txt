cmake_minimum_required (VERSION 2.6...4.1)

project (reiserfs-toys)

add_library (mrfsu STATIC
	../reiserfs.cpp
	../journal.cpp
	../bitmap.cpp
	../block.cpp
	../defrag.cpp
	../progress.cpp
)

add_executable (moveback moveback.cpp)
target_link_libraries(moveback mrfsu)

add_executable (shuffler shuffler.cpp)
target_link_libraries(shuffler mrfsu)
