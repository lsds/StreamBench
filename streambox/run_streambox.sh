#!/bin/sh

echo "Downloading StreamBox..."
wget ftp://ftp.ecn.purdue.edu/xzl/software/streambox/streambox-last.tar.gz
tar -xvzf streambox-last.tar.gz 

echo "Copying new Files..."
cp new_files/Unbounded.cpp streambox_release_March_10_2018/streambox/Source/
cp new_files/Unbounded.h streambox_release_March_10_2018/streambox/Source/
cp new_files/UnboundedInMemEvaluator.h streambox_release_March_10_2018/streambox/Source/
cp new_files/YahooMapper.cpp streambox_release_March_10_2018/streambox/Mapper/
cp new_files/YahooMapper.h streambox_release_March_10_2018/streambox/Mapper/
cp new_files/YahooMapperEvaluator.h streambox_release_March_10_2018/streambox/Mapper/
cp new_files/Values.cpp streambox_release_March_10_2018/streambox/
cp new_files/Values.h streambox_release_March_10_2018/streambox/
cp new_files/test-yahoo.cpp streambox_release_March_10_2018/streambox/test/
cp new_files/config.h streambox_release_March_10_2018/streambox/
cp new_files/CMakeLists.txt streambox_release_March_10_2018/streambox/
cp new_files/EvaluationBundleContext.h streambox_release_March_10_2018/streambox/core
cp new_files/YahooBenchmarkSource.cpp streambox_release_March_10_2018/streambox/Source/
cp new_files/YahooBenchmarkSource.h streambox_release_March_10_2018/streambox/Source/
cp new_files/YahooBenchmarkSourceEvaluator.h streambox_release_March_10_2018/streambox/Source/
cp new_files/SimpleSelect.h streambox_release_March_10_2018/streambox/Select/


echo "Installing Dependencies..."
sudo apt-get install \
    g++ \
    libtbb-dev \
    automake \
    autoconf \
    autoconf-archive \
    libtool \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev;

echo "Compiling benchmark..."
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PATH_TO_SB=$DIR/streambox_release_March_10_2018/streambox
cd $PATH_TO_SB
/usr/bin/cmake -DCMAKE_BUILD_TYPE=Release -G "CodeBlocks - Unix Makefiles" $PATH_TO_SB
make test-yahoo.bin

echo "Running benchmark..."
cd $PATH_TO_SB/
./test-yahoo.bin