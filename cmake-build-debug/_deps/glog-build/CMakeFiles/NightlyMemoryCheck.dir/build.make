# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.17

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Disable VCS-based implicit rules.
% : %,v


# Disable VCS-based implicit rules.
% : RCS/%


# Disable VCS-based implicit rules.
% : RCS/%,v


# Disable VCS-based implicit rules.
% : SCCS/s.%


# Disable VCS-based implicit rules.
% : s.%


.SUFFIXES: .hpux_make_needs_suffix_list


# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /home/zhangqian/CLion-2020.2.4/clion-2020.2.4/bin/cmake/linux/bin/cmake

# The command to remove a file.
RM = /home/zhangqian/CLion-2020.2.4/clion-2020.2.4/bin/cmake/linux/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/zhangqian/paper-tests/stage

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/zhangqian/paper-tests/stage/cmake-build-debug

# Utility rule file for NightlyMemoryCheck.

# Include the progress variables for this target.
include _deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/progress.make

_deps/glog-build/CMakeFiles/NightlyMemoryCheck:
	cd /home/zhangqian/paper-tests/stage/cmake-build-debug/_deps/glog-build && /home/zhangqian/CLion-2020.2.4/clion-2020.2.4/bin/cmake/linux/bin/ctest -D NightlyMemoryCheck

NightlyMemoryCheck: _deps/glog-build/CMakeFiles/NightlyMemoryCheck
NightlyMemoryCheck: _deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/build.make

.PHONY : NightlyMemoryCheck

# Rule to build all files generated by this target.
_deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/build: NightlyMemoryCheck

.PHONY : _deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/build

_deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/clean:
	cd /home/zhangqian/paper-tests/stage/cmake-build-debug/_deps/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/NightlyMemoryCheck.dir/cmake_clean.cmake
.PHONY : _deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/clean

_deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/depend:
	cd /home/zhangqian/paper-tests/stage/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zhangqian/paper-tests/stage /home/zhangqian/paper-tests/stage/cmake-build-debug/_deps/glog-src /home/zhangqian/paper-tests/stage/cmake-build-debug /home/zhangqian/paper-tests/stage/cmake-build-debug/_deps/glog-build /home/zhangqian/paper-tests/stage/cmake-build-debug/_deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : _deps/glog-build/CMakeFiles/NightlyMemoryCheck.dir/depend

