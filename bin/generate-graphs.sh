#!/bin/bash

# Copyright (c) 2013, Regents of the University of California
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.  Redistributions in binary
# form must reproduce the above copyright notice, this list of conditions and the
# following disclaimer in the documentation and/or other materials provided with
# the distribution.  Neither the name of the University of California, Berkeley
# nor the names of its contributors may be used to endorse or promote products
# derived from this software without specific prior written permission.  THIS
# SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

bin_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CLUSTER_SIM_HOME=$bin_dir/..
echo CLUSTER_SIM_HOME is $CLUSTER_SIM_HOME/src/main/python/graphing-scripts
cd $CLUSTER_SIM_HOME/src/main/python/graphing-scripts

function usage
{
  echo "usage: `basename $0` ABS_PATH_TO_INPUT_DIR [--env-set ENV_SET_1 --env-set ENV_SET_2 --png --paper-mode]"
}

if [ $# -eq 0 ]; then
  echo "please provide the input-directory containing your protocol buffer files (ending in .protobuf)"
  usage
  exit
fi
input_dir=$1
shift
echo input_dir set as $input_dir
run_time='86400'
# do_png should be "" or "png"
do_png=''
# 0 = normal, 1 = paper
modes=0
# Used to name directories generated in graph directory.
# Only affects which lines are actually plotted when in paper-mode.
# Must be all caps.
env_sets_to_plot=''

while [ "$1" != "" ]; do
  case $1 in
    -e | --env_set )         env_sets_to_plot+=$1
                             ;;
    -p | --png )             do_png="png"
                             ;;
    --paper-mode )           modes+=1
                             ;;
    -h | --help )            usage
                             exit
                             ;;
    * )                      usage
                             exit 1
  esac
  shift
done

case $input_dir in
  *vary_C*)      vary_dimensions+=c;;
  *vary_L*)      vary_dimensions+=l;;
  *vary_Lambda*) vary_dimensions+=lambda;;
  *)             echo "Protobuf filename must contain \"vary_[C|L|Lambda]\"."
                 exit 1
esac

# Use a default env_set if none was specified.
if [ -z "$env_sets_to_plot" ]; then
  env_sets_to_plot='C'
fi

plotting_script='generate-plots-from-protobuff.py'

# Assumes runtime is the last token of the filename.
run_time=`echo ${input_dir} | grep '[0-9]\+$' --only-matching`

function graph_experiment() {
  if [ -z "$1" ]; then # Is parameter #1 zero length?
    echo "graph_experiment requires 1 parameter (the protobuff file name)."
    exit
  fi
  filename=$1

  for mode in $modes; do
    echo mode is ${mode} '(0 = non-paper, 1 = paper)'
    if [[ ${mode} -eq 1 ]]; then
      out_dir="${input_dir}/graphs/paper"
    else
      out_dir="${input_dir}/graphs"
    fi

    # Figure out which simulator type this protobuff came from.
    case $filename in
      *omega-resource-fit-incremental*)         sim=omega-resource-fit-incremental;;
      *omega-resource-fit-all-or-nothing*)      sim=omega-resource-fit-all-or-nothing;;
      *omega-sequence-numbers-incremental*)     sim=omega-sequence-numbers-incremental;;
      *omega-sequence-numbers-all-or-nothing*)  sim=omega-sequence-numbers-all-or-nothing;;
      *monolithic*)                             sim=monolithic;;
      *mesos*)                                  sim=mesos;;
      *)                                        echo "Unknown simulator type, in ${filename} exiting."
                                                exit 1
    esac

    num_service_scheds=`echo ${filename} | \
                        grep --only-matching '[0-9]\+_service' | \
                        grep --only-matching '[0-9]\+'`
    num_batch_scheds=`echo ${filename} | \
                        grep --only-matching '[0-9]\+_batch' | \
                        grep --only-matching '[0-9]\+'`
    echo "Parsed filename for num service (${num_service_scheds}) and" \
         "batch (${num_batch_scheds}) schedulers."
    for vd in ${vary_dimensions}; do
      echo generating graphs for dimension ${vd}.

      case $filename in
        *single_path*) pathness=single_path;;
        *multi_path*)  pathness=multi_path;;
        *)             echo "Protobuf filename must contain"         \
                            "[single|multi]_path."
                       exit 1
      esac
      echo Pathness is ${pathness}.

      for envs_to_plot in ${env_sets_to_plot}; do
        complete_out_dir=${out_dir}/${sim}/${pathness}/${envs_to_plot}/${num_service_scheds}_service-${num_batch_scheds}_batch
        mkdir -p ${complete_out_dir}
        echo 'PYTHONPATH=$PYTHONPATH:.. '"python ${plotting_script}" \
           "${complete_out_dir}"                                       \
           "${input_dir}/${filename}"                                \
           "${mode} ${vd} ${envs_to_plot} ${do_png}"
        PYTHONPATH=$PYTHONPATH:.. python ${plotting_script}          \
            ${complete_out_dir}                                        \
            ${input_dir}/${filename}                                 \
            ${mode} ${vd} ${envs_to_plot} ${do_png}
        echo -e "\n"
      done
    done
  done
}

PROTO_LIST=''
echo capturing: ls $input_dir|grep protobuf
ls $input_dir|grep protobuf
for curr_filename in `ls $input_dir|grep protobuf`; do
  PROTO_LIST+=curr_filename
done
echo Calling graph_experiment with $curr_filename
graph_experiment $curr_filename

