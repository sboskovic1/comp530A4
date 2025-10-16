#!/bin/bash
clear
cd /storage-home/t/tl107/comp432/comp530A4/Build
echo 8 | ~/scons/bin/scons-3.1.2
if [ -n "$1" ]; then
    bin/bPlusUnitTest > "../$1"
else
    bin/bPlusUnitTest
fi