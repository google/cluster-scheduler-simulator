#!/usr/bin/python

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

# This file generates a set of graphs for a simulator "experiment".
# An experiment is equivalent to the file generated from the run of a
# single Experiment object in the simulator (i.e. a parameter sweep for a
# set of workload_descs), with the added constraint that only one of
# C, L, or lambda can be varied per a single series (the simulator
# currently allows ranges to be provided for more than one of these).

import sys, os, re
from utils import *
import numpy as np
import matplotlib.pyplot as plt
import math
import operator
import logging
from collections import defaultdict

import cluster_simulation_protos_pb2

logging.basicConfig(level=logging.DEBUG, format="%(message)s")

def usage():
  print "usage: scheduler-business.py <output folder> <REMOVED: input_protobuff> " \
      "<paper_mode: 0|1> <vary_dim: c|l|lambda> <env: any of A,B,C> [png]"
  sys.exit(1)

# if len(sys.argv) < 6:
#   logging.error("Not enough arguments provided.")
#   usage()
 
paper_mode = True
output_formats = ['pdf']
# try:
#   output_prefix = str(sys.argv[1])
#   input_protobuff = sys.argv[2]
#   if int(sys.argv[3]) == 1:
#     paper_mode = True
#   vary_dim = sys.argv[4]
#   if vary_dim not in ['c', 'l', 'lambda']:
#     logging.error("vary_dim must be c, l, or lambda!")
#     sys.exit(1)
#   envs_to_plot = sys.argv[5]
#   if re.search("[^ABC]",envs_to_plot):
#     logging.error("envs_to_plot must be any combination of a, b, and c, without spaces!")
#     sys.exit(1)
#   if len(sys.argv) == 7:
#     if sys.argv[6] == "png":
#       output_formats.append('png')
#     else:
#       logging.error("The only valid optional 5th argument is 'png'")
#       sys.exit(1)
# 
# except:
#   usage()
# 
# set_leg_fontsize(11)

# logging.info("Output prefix: %s" % output_prefix)
# logging.info("Input file: %s" % input_protobuff)

# google-omega-resfit-allornoth-single_path-vary_l-604800.protobuf
# google-omega-resfit-inc-single_path-vary_l-604800.protobuf
# google-omega-seqnum-allornoth-single_path-vary_l-604800.protobuf
# google-omega-seqnum-inc-single_path-vary_l-604800.protobuf

envs_to_plot = "C"

file_dir = '/Users/andyk/omega-7day-simulator-results/'
output_prefix = file_dir + "/graphs"

file_names = [("Fine/Gang", "google-omega-resfit-allornoth-single_path-vary_c-604800.protobuf"),  
              ("Fine/Inc", "google-omega-resfit-inc-single_path-vary_c-604800.protobuf"),
              ("Coarse/Gang", "google-omega-seqnum-allornoth-single_path-vary_c-604800.protobuf"),
              ("Course/Inc", "google-omega-seqnum-inc-single_path-vary_c-604800.protobuf")]

experiment_result_sets = []
for title_name_tuple in file_names:
  title = title_name_tuple[0]
  file_name = title_name_tuple[1]
  full_name = file_dir + file_name
  # Read in the ExperimentResultSet.
  #experiment_result_sets.append((title, cluster_simulation_protos_pb2.ExperimentResultSet()))
  res_set = cluster_simulation_protos_pb2.ExperimentResultSet()
  experiment_result_sets.append([title, res_set])
  #titles[experiment_result_sets[-1]] = title
  f = open(full_name, "rb")
  res_set.ParseFromString(f.read())
  f.close()


# ---------------------------------------
# Set up some general graphing variables.
if paper_mode:
  set_paper_rcs()
  fig = plt.figure(figsize=(2,1.33))
else:
  fig = plt.figure()

prefilled_colors_web = { 'A': 'b', 'B': 'r', 'C': 'c', "synth": 'y' }
colors_web = { 'A': 'b', 'B': 'r', 'C': 'm', "synth": 'y' }
colors_paper = { 'A': 'b', 'B': 'r', 'C': 'c', "synth": 'b' }
per_wl_colors = { 'OmegaBatch': 'b',
                  'OmegaService': 'r' }

title_colors_web = { "Fine/Gang": 'b', "Fine/Inc": 'r', "Coarse/Gang": 'm', "Course/Inc": 'c' }

prefilled_linestyles_web = { 'Monolithic': 'D-',
                   'MonolithicApprox': 's-',
                   'MesosBatch': 'D-',
                   'MesosService': 'D:',
                   'MesosBatchApprox': 's-',
                   'MesosServiceApprox': 's:',
                   'OmegaBatch': 'D-',
                   'OmegaService': 'D:',
                   'OmegaBatchApprox': 's-',
                   'OmegaServiceApprox': 's:',
                   'Batch': 'D-',
                   'Service': 'D:' }

linestyles_web = { 'Monolithic': 'x-',
                   'MonolithicApprox': 'o-',
                   'MesosBatch': 'x-',
                   'MesosService': 'x:',
                   'MesosBatchApprox': 'o-',
                   'MesosServiceApprox': 'o:',
                   'OmegaBatch': 'x-',
                   'OmegaService': 'x:',
                   'OmegaBatchApprox': 'o-',
                   'OmegaServiceApprox': 'o:',
                   'Batch': 'x-',
                   'Service': 'x:' }
linestyles_paper = { 'Monolithic': '-',
                     'MonolithicApprox': '--',
                     'MesosBatch': '-',
                     'MesosService': ':',
                     'MesosBatchApprox': '--',
                     'MesosServiceApprox': '-.',
                     'OmegaBatch': '-',
                     'OmegaService': ':',
                     'OmegaBatchApprox': '--',
                     'OmegaServiceApprox': '-.',
                     'Batch': '-',
                     'Service': ':' }

dashes_paper = { 'Monolithic': (None,None),
                 'MonolithicApprox': (3,3),
                 'MesosBatch': (None,None),
                 'MesosService': (1,1),
                 'MesosBatchApprox': (3,3),
                 'MesosServiceApprox': (4,2),
                 'OmegaBatch': (None,None),
                 'OmegaService': (1,1),
                 'OmegaBatchApprox': (3,3),
                 'OmegaServiceApprox': (4,2),
                 'Batch': (None,None),
                 'Service': (1,1),
                 'Fine/Gang': (1,1),       
                 'Fine/Inc': (3,3), 
                 'Coarse/Gang': (4,2)
               }

# Some dictionaries whose values will be dictionaries
# to make 2d dictionaries, which will be indexed by both exp_env 
# and either workoad or scheduler name.
# --
# (cellName, assignmentPolicy, workload_name) -> array of data points
# for the parameter sweep done in the experiment.
workload_queue_time_till_first = {}
workload_queue_time_till_fully = {}
workload_queue_time_till_first_90_ptile = {}
workload_queue_time_till_fully_90_ptile = {}
workload_num_jobs_unscheduled = {}
# (cellName, assignmentPolicy, scheduler_name) -> array of data points
# for the parameter sweep done in the experiment.
sched_total_busy_fraction = {}
sched_daily_busy_fraction = {}
sched_daily_busy_fraction_err = {}
# TODO(andyk): Graph retry_busy_fraction on same graph as total_busy_fraction
#              to parallel Malte's graphs.
# sched_retry_busy_fraction = {}
sched_conflict_fraction = {}
sched_daily_conflict_fraction = {}
sched_daily_conflict_fraction_err = {}
sched_task_conflict_fraction = {}
sched_num_retried_transactions = {}
sched_num_jobs_remaining = {}
sched_failed_find_victim_attempts = {}

# Convenience wrapper to override __str__()
class ExperimentEnv:
  def __init__(self, init_exp_env):
    self.exp_env = init_exp_env
    self.cell_name = init_exp_env.cell_name
    self.workload_split_type = init_exp_env.workload_split_type
    self.is_prefilled = init_exp_env.is_prefilled
    self.run_time = init_exp_env.run_time

  def __str__(self):
    return str("%s, %s" % (self.exp_env.cell_name, self.exp_env.workload_split_type))

  # Figure out if we are varying c, l, or lambda in this experiment.
  def vary_dim(self):
    env = self.exp_env # Make a convenient short handle.
    assert(len(env.experiment_result) > 1)
    if (env.experiment_result[0].constant_think_time !=
        env.experiment_result[1].constant_think_time):
      vary_dim = "c"
      # logging.debug("Varying %s. The first two experiments' c values were %d, %d "
      #               % (vary_dim,
      #                  env.experiment_result[0].constant_think_time,
      #                  env.experiment_result[1].constant_think_time))
    elif (env.experiment_result[0].per_task_think_time !=
          env.experiment_result[1].per_task_think_time):
      vary_dim = "l"
      # logging.debug("Varying %s. The first two experiments' l values were %d, %d "
      #               % (vary_dim,
      #                  env.experiment_result[0].per_task_think_time,
      #                  env.experiment_result[1].per_task_think_time))
    else:
      vary_dim = "lambda"
    # logging.debug("Varying %s." % vary_dim)
    return vary_dim
  
class Value:
  def __init__(self, init_x, init_y):
    self.x = init_x
    self.y = init_y
  def __str__(self):
    return str("%f, %f" % (self.x, self.y))

def bt_approx(cell_name, sched_name, point, vary_dim_, tt_c, tt_l, runtime):
  logging.debug("sched_name is %s " % sched_name)
  assert(sched_name == "Batch" or sched_name == "Service")
  lbd = {}
  n = {}
  # This function calculates an approximated scheduler busyness line given
  # an average inter-arrival time and job size for each scheduler
  # XXX: configure the below parameters and comment out the following
  # line in order to
  # 1) disable the warning, and
  # 2) get a correct no-conflict approximation.
  print >> sys.stderr, "*********************************************\n" \
      "WARNING: YOU HAVE NOT CONFIGURED THE PARAMETERS IN THE bt_approx\n" \
      "*********************************************\n"
  ################################
  # XXX EDIT BELOW HERE
  # hard-coded SAMPLE params for cluster A
  lbd['A'] = { "Batch": 0.1, "Service": 0.01 } # lambdas for 0: serv & 1: Batch 
  n['A'] = { "Batch": 10.0, "Service": 5.0 } # avg num tasks per job
  # hard-coded SAMPLE params for cluster B
  lbd['B'] = { "Batch": 0.1, "Service": 0.01 }
  n['B'] = { "Batch": 10.0, "Service": 5.0 }
  # hard-coded SAMPLE params for cluster C
  lbd['C'] = { "Batch": 0.1, "Service": 0.01 }
  n['C'] = { "Batch": 10.0, "Service": 5.0 }
  ################################

  # approximation formula
  if vary_dim_ == 'c':
    # busy_time = num_jobs * (per_job_think_time = C + nL) / runtime
    return runtime * lbd[cell_name][sched_name] *                       \
           ((point + n[cell_name][sched_name] * float(tt_l))) / runtime
  elif vary_dim_ == 'l':
    return runtime * lbd[cell_name][sched_name] *                       \
           ((float(tt_c) + n[cell_name][sched_name] * point)) / runtime

def get_mad(median, data):
  #print "in get_mad, with median %f, data: %s" % (median, " ".join([str(i) for i in data]))
  devs = [abs(x - median) for x in data]
  mad = np.median(devs)
  #print "returning mad = %f" % mad
  return mad

def sort_labels(handles, labels):
  hl = sorted(zip(handles, labels),
              key=operator.itemgetter(1))
  handles2, labels2 = zip(*hl)
  return (handles2, labels2)

for experiment_result_set_arry in experiment_result_sets:
  title = experiment_result_set_arry[0]
  logging.debug("\n\n==========================\nHandling title %s." % title)
  experiment_result_set = experiment_result_set_arry[1]

  # Loop through each experiment environment.
  logging.debug("Processing %d experiment envs."
                % len(experiment_result_set.experiment_env))
  for env in experiment_result_set.experiment_env:
    if not re.search(cell_to_anon(env.cell_name), envs_to_plot):
      logging.debug("  skipping env/cell " + env.cell_name)
      continue
    logging.debug("\n\n\n env: " + env.cell_name)
    exp_env = ExperimentEnv(env) # Wrap the protobuff object to get __str__()
    logging.debug("  Handling experiment env %s." % exp_env)
  
    # Within this environment, loop through each experiment result
    logging.debug("  Processing %d experiment results." % len(env.experiment_result))
    for exp_result in env.experiment_result:
      logging.debug("    Handling experiment with per_task_think_time %f, constant_think_time %f"
            % (exp_result.per_task_think_time, exp_result.constant_think_time))
      # Record the correct x val depending on which dimension is being
      # swept over in this experiment.
      vary_dim = exp_env.vary_dim() # This line is unecessary since this value 
                                    # is a flag passed as an arg to the script.
      if vary_dim == "c":
        x_val = exp_result.constant_think_time
      elif vary_dim == "l":
        x_val = exp_result.per_task_think_time
      else:
        x_val = exp_result.avg_job_interarrival_time
      # logging.debug("Set x_val to %f." % x_val)
  
      # Build results dictionaries of per-scheduler stats.
      for sched_stat in exp_result.scheduler_stats:
        # Per day busy time and conflict fractions.
        daily_busy_fractions = []
        daily_conflict_fractions = []
        daily_conflicts = [] # counts the mean of daily abs # of conflicts.
        daily_successes = []
        logging.debug("      handling scheduler %s" % sched_stat.scheduler_name)
        for day_stats in sched_stat.per_day_stats:
          # Calculate the total busy time for each of the days and then
          # take median of all fo them.
          run_time_for_day = exp_env.run_time - 86400 * day_stats.day_num
          # logging.debug("setting run_time_for_day = exp_env.run_time - 86400 * "
          #       "day_stats.day_num = %f - 86400 * %d = %f"
          #       % (exp_env.run_time, day_stats.day_num, run_time_for_day))
          if run_time_for_day > 0.0:
            daily_busy_fractions.append(((day_stats.useful_busy_time +
                                          day_stats.wasted_busy_time) /
                                         min(86400.0, run_time_for_day)))
  
            if day_stats.num_successful_transactions > 0:
              conflict_fraction = (float(day_stats.num_failed_transactions) /
                                   float(day_stats.num_successful_transactions))
              daily_conflict_fractions.append(conflict_fraction)
              daily_conflicts.append(float(day_stats.num_failed_transactions))
              daily_successes.append(float(day_stats.num_successful_transactions))
              # logging.debug("appending daily_conflict_fraction %f / %f = %f." 
              #       % (float(day_stats.num_failed_transactions),
              #          float(day_stats.num_successful_transactions),
              #          conflict_fraction))
            else:
              daily_conflict_fractions.append(0)

        # Daily busy time median.
        daily_busy_time_med = np.median(daily_busy_fractions)
        logging.debug("      Daily_busy_fractions, med: %f, vals: %s"
              % (daily_busy_time_med,
                 " ".join([str(i) for i in daily_busy_fractions])))
        value = Value(x_val, daily_busy_time_med)
        append_or_create_2d(sched_daily_busy_fraction,
                            title,
                            sched_stat.scheduler_name,
                            value)
        #logging.debug("sched_daily_busy_fraction[%s %s].append(%s)."
        #              % (exp_env, sched_stat.scheduler_name, value))
        # Error Bar (MAD) for daily busy time.
        value = Value(x_val, get_mad(daily_busy_time_med,
                                     daily_busy_fractions))
        append_or_create_2d(sched_daily_busy_fraction_err,
                            title,
                            sched_stat.scheduler_name,
                            value)
        #logging.debug("sched_daily_busy_fraction_err[%s %s].append(%s)."
        #              % (exp_env, sched_stat.scheduler_name, value))
        # Daily conflict fraction median.
        daily_conflict_fraction_med = np.median(daily_conflict_fractions)
        logging.debug("      Daily_abs_num_conflicts, med: %f, vals: %s"
              % (np.median(daily_conflicts),
                 " ".join([str(i) for i in daily_conflicts])))
        logging.debug("      Daily_num_successful_conflicts, med: %f, vals: %s"
              % (np.median(daily_successes),
                 " ".join([str(i) for i in daily_successes])))
        logging.debug("      Daily_conflict_fractions, med : %f, vals: %s\n      --"
              % (daily_conflict_fraction_med,
                 " ".join([str(i) for i in daily_conflict_fractions])))
        value = Value(x_val, daily_conflict_fraction_med)
        append_or_create_2d(sched_daily_conflict_fraction,
                            title,
                            sched_stat.scheduler_name,
                            value)
        # logging.debug("sched_daily_conflict_fraction[%s %s].append(%s)."
        #               % (exp_env, sched_stat.scheduler_name, value))
        # Error Bar (MAD) for daily conflict fraction.
        value = Value(x_val, get_mad(daily_conflict_fraction_med,
                                     daily_conflict_fractions))
        append_or_create_2d(sched_daily_conflict_fraction_err,
                            title,
                            sched_stat.scheduler_name,
                            value)
        

def plot_2d_data_set_dict(data_set_2d_dict,
                          plot_title,
                          filename_suffix,
                          y_label,
                          y_axis_type,
                          error_bars_data_set_2d_dict = None):
  assert(y_axis_type == "0-to-1" or
         y_axis_type == "ms-to-day" or 
         y_axis_type == "abs")
  plt.clf()
  ax = fig.add_subplot(111)
  for title, name_to_val_map in data_set_2d_dict.iteritems():
    for wl_or_sched_name, values in name_to_val_map.iteritems():
      line_label = title
      # Hacky: chop MonolithicBatch, MesosBatch, MonolithicService, etc.
      # down to "Batch" and "Service" if in paper mode.
      updated_wl_or_sched_name = wl_or_sched_name
      if paper_mode and re.search("Batch", wl_or_sched_name):
        updated_wl_or_sched_name = "Batch"
      if paper_mode and re.search("Service", wl_or_sched_name):
        updated_wl_or_sched_name = "Service"

      # Don't show lines for service frameworks
      if updated_wl_or_sched_name == "Batch":
        "Skipping a line for a service scheduler"
        continue
      x_vals = [value.x for value in values]
      # Rewrite zero's for the y_axis_types that will be log.
      y_vals = [0.00001 if (value.y == 0 and y_axis_type == "ms-to-day")
                        else value.y for value in values]
      logging.debug("Plotting line for %s %s %s." %
                    (title, updated_wl_or_sched_name, plot_title))
      #logging.debug("x vals: " + " ".join([str(i) for i in x_vals]))
      #logging.debug("y vals: " + " ".join([str(i) for i in y_vals]))
      logging.debug("wl_or_sched_name: " + wl_or_sched_name)
      logging.debug("title: " + title)

      ax.plot(x_vals, y_vals,
              dashes=dashes_paper[wl_or_sched_name],
              color=title_colors_web[title],
              label=line_label, markersize=4,
              mec=title_colors_web[title])

  setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type)

def setup_graph_details(ax, plot_title, filename_suffix, y_label, y_axis_type):
  assert(y_axis_type == "0-to-1" or
         y_axis_type == "ms-to-day" or 
         y_axis_type == "abs")

  # Paper title.
  if not paper_mode:
    plt.title(plot_title)

  if paper_mode:
    try:
      # Set up the legend, for removing the border if in paper mode.
      handles, labels = ax.get_legend_handles_labels()
      handles2, labels2 = sort_labels(handles, labels)
      leg = plt.legend(handles2, labels2, loc=2, labelspacing=0)
      fr = leg.get_frame()
      fr.set_linewidth(0)
    except:
      print "Failed to remove frame around legend, legend probably is empty."

  # Axis labels.
  if not paper_mode:
    ax.set_ylabel(y_label)
    if vary_dim == "c":
      ax.set_xlabel(u'Scheduler 1 constant processing time [sec]')
    elif vary_dim == "l":
      ax.set_xlabel(u'Scheduler 1 per-task processing time [sec]')
    elif vary_dim == "lambda":
      ax.set_xlabel(u'Job arrival rate to scheduler 1, $\lambda_1$')

  # x-axis scale, limit, tics and tic labels.
  ax.set_xscale('log')
  ax.set_autoscalex_on(False)
  if vary_dim == 'c':
    plt.xlim(xmin=0.01)
    plt.xticks((0.01, 0.1, 1, 10, 100), ('10ms', '0.1s', '1s', '10s', '100s'))
  elif vary_dim == 'l':
    plt.xlim(xmin=0.001, xmax=1)
    plt.xticks((0.001, 0.01, 0.1, 1), ('1ms', '10ms', '0.1s', '1s'))
  elif vary_dim == 'lambda':
    plt.xlim([0.1, 100])
    plt.xticks((0.1, 1, 10, 100), ('0.1s', '1s', '10s', '100s'))

  # y-axis limit, tics and tic labels.
  if y_axis_type == "0-to-1":
    logging.debug("Setting up y-axis for '0-to-1' style graph.")
    plt.ylim([0, 1])
    plt.yticks((0, 0.2, 0.4, 0.6, 0.8, 1.0),
               ('0.0', '0.2', '0.4', '0.6', '0.8', '1.0'))
  elif y_axis_type == "ms-to-day":
    logging.debug("Setting up y-axis for 'ms-to-day' style graph.")
    #ax.set_yscale('symlog', linthreshy=0.001)
    ax.set_yscale('log')
    plt.ylim(ymin=0.01, ymax=24*3600)
    plt.yticks((0.01, 1, 60, 3600, 24*3600), ('10ms', '1s', '1m', '1h', '1d'))
  elif y_axis_type == "abs":
    plt.ylim(ymin=0)
    logging.debug("Setting up y-axis for 'abs' style graph.")
    #plt.yticks((0.01, 1, 60, 3600, 24*3600), ('10ms', '1s', '1m', '1h', '1d'))
  else:
    logging.error('y_axis_label parameter must be either "0-to-1"'
                  ', "ms-to-day", or "abs".')
    sys.exit(1)

  final_filename = (output_prefix +
                   ('/sisi-vary-%s-vs-' % vary_dim) +
                   filename_suffix)
  logging.debug("Writing plot to %s", final_filename)
  writeout(final_filename, output_formats)


#SCHEDULER DAILY BUSY AND CONFLICT FRACTION MEDIANS
plot_2d_data_set_dict(sched_daily_busy_fraction,
                      "Scheduler processing time vs. median(daily busy time fraction)",
                      "daily-busy-fraction-med",
                      u'Median(daily busy time fraction)',
                      "0-to-1")

plot_2d_data_set_dict(sched_daily_conflict_fraction,
                      "Scheduler processing time vs. median(daily conflict fraction)",
                      "daily-conflict-fraction-med",
                      u'Median(daily conflict fraction)',
                      "0-to-1")
