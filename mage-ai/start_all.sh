#!/bin/bash
set -e

echo "Starting Kafka consumer in background..."
cd /home/src/techpulse_intelligence
python kafka_consumer.py &

echo "Starting periodic sync in background..."
python periodic_sync.py &

echo "Starting Mage AI server..."
exec mage start /home/src/techpulse_intelligence --project-type standalone
