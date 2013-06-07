#!/bin/bash

curr_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $curr_dir

dirname=$1
echo dirname is $dirname

if [[ ! -d $dirname ]]; then
 echo "Error: accepts a single argument which is dirname containing protobuffs."
 exit
fi

# TODO(andyk): make naming convention for protobuf(f) consistent.
for i in `ls $dirname/*.protobuf`; do
  echo PYTHONPATH=$PYTHONPATH:.. python ./generate-txt-from-protobuff.py $i 
  PYTHONPATH=$PYTHONPATH:.. python ./generate-txt-from-protobuff.py $i 
done
