#!/usr/bin/env bash

cd /tmp
wget https://downloads.apache.org/arrow/arrow-4.0.1/apache-arrow-4.0.1.tar.gz
tar -zxvf apache-arrow-4.0.1.tar.gz
cd apache-arrow-4.0.1

cd cpp
mkdir release
cd release
cmake -DCMAKE_INSTALL_PREFIX=/usr/local/ -DCMAKE_BUILD_TYPE=Release -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j$(nproc)
sudo make install -j$(nproc)
sudo cp /tmp/apache-arrow-4.0.1/cpp/release/release/libplasma_java.so /usr/local/lib/
sudo cp /tmp/apache-arrow-4.0.1/cpp/release/release/libplasma_java.so /usr/lib/
