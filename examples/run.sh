#!/bin/bash

#export LD_LIBRARY_PATH=$HOME/simgrid-3.11.1/lib

#./hello_bighybrid.bin --cfg=surf/nthreads:-1 2>&1 | $HOME/simgrid-3.11.1/bin/simgrid-colorizer

export LD_LIBRARY_PATH=/opt/simgrid/lib64

clear
cd ..
make clean all
cd examples/
make clean all
rm times.txt
touch times.txt

./hello_bighybrid.bin platforms/plat350-350.xml platforms/d-plat350-350.xml bighyb-plat350-350.conf volatiles/parser-boinc-080.txt