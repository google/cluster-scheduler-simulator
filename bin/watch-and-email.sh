#! /bin/bash

PROCESS_ID=$@

echo monitoring process $PROCESS_ID

IS_FINISHED=0

function test_is_finished() {
  ps -p $PROCESS_ID
  IS_FINISHED=$?
}

test_is_finished

while [[ $IS_FINISHED -eq 0 ]]; do
  echo Process $PROCESS_ID still running, waiting to send email.
  sleep 10
  test_is_finished
done

echo Process $PROCESS_ID done running, sending email.

echo "Cluster Simulation experiments with pid $PROCESS_ID just finished running on `hostname`!" | mail -s  "Cluster Simulation pid $PROCESS_ID finished running!" andykonwinski@gmail.com
