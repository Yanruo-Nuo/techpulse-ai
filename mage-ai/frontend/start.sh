#!/bin/bash
set -e

# Start Prometheus metrics server independently (doesn't depend on Streamlit)
echo "Starting Prometheus metrics server on port 8003..."
python /app/metrics_collector.py &
echo "Metrics server PID: $!"

echo "Starting Streamlit..."
exec streamlit run app.py --server.port=8501 --server.address=0.0.0.0
