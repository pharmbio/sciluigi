#!/bin/bash
TESTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "NOTICE: this script might not work properly you have CDPATH set to something that contains a folder named 'test'"
python -m unittest discover $TESTDIR
