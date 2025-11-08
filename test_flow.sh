#!/bin/bash
# Create this file: test_flow.sh

echo "--- QueueCTL Test Flow ---"

# Ensure clean state
echo "[1] Stopping any running workers..."
queuectl worker stop
rm -f ~/.queuectl/jobs.db

echo "\n[2] Setting config..."
queuectl config set max_retries 2
queuectl config set backoff_base 2
queuectl config show

echo "\n[3] Enqueuing jobs..."
# Successful job
queuectl enqueue '{"id":"job_success", "command":"echo \"Job 1 Success!\""}'
# Failing job that will retry and go to DLQ
queuectl enqueue '{"id":"job_fail", "command":"exit 1"}'
# Invalid command job
queuectl enqueue '{"id":"job_invalid", "command":"thiscommanddoesnotexist"}'
# Long-running job
queuectl enqueue '{"id":"job_sleep", "command":"sleep 4"}'

echo "\n[4] Checking initial status..."
queuectl status
queuectl list --state pending

echo "\n[5] Starting 2 workers in background..."
queuectl worker start --count 2
sleep 2 # Give workers time to start
queuectl status

echo "\n[6] Waiting for jobs to process (approx 10 seconds)..."
sleep 10

echo "\n[7] Checking final status..."
queuectl status

echo "\n[8] Verifying outcomes..."
echo "--- Completed Jobs ---"
queuectl list --state completed
echo "--- DLQ Jobs ---"
queuectl dlq list

echo "\n[9] Retrying job_fail from DLQ..."
queuectl dlq retry job_fail
queuectl dlq list
queuectl list --state pending

echo "\n[10] Waiting 2s for retry..."
sleep 2
queuectl status

echo "\n[11] Stopping workers..."
queuectl worker stop
queuectl status

echo "\n--- Test Flow Complete ---"