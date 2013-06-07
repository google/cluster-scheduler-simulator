#!/bin/bash

curr_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $curr_dir

PYTHONPATH=$PYTHONPATH:.. python ./comparison-plot-from-protobuff.py $@
