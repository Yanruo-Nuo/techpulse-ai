#!/bin/bash
# 不 set -e: 后台进程崩溃不应导致整个脚本退出
set -u

echo "Starting Kafka consumer in background (auto-restart on crash)..."
cd /home/src/techpulse_intelligence
(
  while true; do
    python kafka_consumer.py
    EXIT_CODE=$?
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] kafka_consumer.py exited with code $EXIT_CODE, restarting in 5s..."
    sleep 5
  done
) &
CONSUMER_PID=$!
echo "  → PID $CONSUMER_PID"

echo "Starting periodic sync in background..."
(
  while true; do
    python periodic_sync.py
    EXIT_CODE=$?
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] periodic_sync.py exited with code $EXIT_CODE, restarting in 10s..."
    sleep 10
  done
) &
SYNC_PID=$!
echo "  → PID $SYNC_PID"

echo "Starting Mage AI server..."
exec mage start /home/src/techpulse_intelligence --project-type standalone
