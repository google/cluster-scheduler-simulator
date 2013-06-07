#!/bin/bash

curr_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $curr_dir

protoc --java_out=../java --python_out=../python ./cluster_simulation_protos.proto
