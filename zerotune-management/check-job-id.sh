#!/bin/bash

# This script checks periodically if the list of running job IDs is the same as before and therefore can detecte if PlanGeneratorFlink is stuck. 
# If it is stuck, it will delete the existing task manager and send a Telegram message.
# The script is intended to be run on the Kubernetes master node in a background process (e.g. screen -S check-job-id -d -m ./check-job-id.sh)
# The script requires the following tools to be installed: curl, jq
# The script requires the following environment variables to be set: FLINK_REST_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


############
## CONFIG ##
############

# Set the URL of the Flink REST API
FLINK_REST_URL="http://w.x.y.z:8081"

# Set the Telegram bot token and chat ID
TELEGRAM_BOT_TOKEN="123:ABC"
TELEGRAM_CHAT_ID="123"

################
## CONFIG END ##
################




# Set the amount of time to wait between checks (in seconds)
WAIT_TIME=20

# Initialize variables
last_job_ids=""
same_job_ids_count=0

while true; do
  # Get the current list of running jobs
  current_job_ids=$(curl -s "${FLINK_REST_URL}/jobs" | jq -r '.jobs[] | select(.status == "RUNNING") | .id')
  echo "Current job ids: $current_job_ids"

  # If the current list of job IDs is the same as the previous list, increment the count
  if [ "$current_job_ids" == "$last_job_ids" ]; then
    same_job_ids_count=$((same_job_ids_count + 1))
    echo "Same job id as before"
  else
    same_job_ids_count=0
    echo "New job id"
  fi

  # If the same list of job IDs has been seen for a period of time send a Telegram message
  if [ "$same_job_ids_count" -ge 30 ]; then
    message="The list of running job IDs has been the same for a longer period:$current_job_ids. We try to delete the existing task manager."
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d chat_id="$TELEGRAM_CHAT_ID" \
      -d text="$message"
    kubectl delete pods -l component=taskmanager -n plangeneratorflink-namespace
    same_job_ids_count=0
  fi

  # Save the current list of job IDs for comparison on the next loop
  last_job_ids="$current_job_ids"

  # Wait for the specified amount of time before checking again
  sleep "$WAIT_TIME"
done