#!/bin/bash
clear
cd /storage-home/s/sb121/comp530/comp530A4/Build
rm core*
echo 8 | ~/scons/bin/scons-3.1.2
bin/bPlusUnitTest
gdb bin/bPlusUnitTest core*