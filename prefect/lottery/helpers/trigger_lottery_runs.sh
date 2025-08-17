#!/bin/bash

# Usage: ./trigger_lottery_runs.sh <start_draw> <end_draw>
# Example: ./trigger_lottery_runs.sh 1 2000

# Validate input arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <start_draw> <end_draw>"
  exit 1
fi

# Assign input arguments to variables
START_DRAW=$1
END_DRAW=$2

# Batch size and wait time
BATCH_SIZE=10
WAIT_TIME=60

# Loop through the range and trigger Prefect deployment runs
CURRENT_BATCH=0
for i in $(seq $START_DRAW $END_DRAW); do
  echo "Triggering run for draw number $i..."
  prefect deployment run lottery-results/lottery-results --param draw_number=$i
  
  # Increment the batch counter
  CURRENT_BATCH=$((CURRENT_BATCH + 1))
  
  # If the batch size is reached, wait for the specified time
  if [ "$CURRENT_BATCH" -ge "$BATCH_SIZE" ]; then
    echo "Batch limit reached. Waiting for $WAIT_TIME seconds before triggering the next batch..."
    sleep $WAIT_TIME
    CURRENT_BATCH=0
  fi
done