#!/usr/bin/env bash
set -euo pipefail

# Usage: scripts/run_producer.sh <kafka_bin_dir> <brokers> <topic>
# Example: scripts/run_producer.sh /opt/kafka/bin localhost:9092 web_logs

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <kafka_bin_dir> <brokers> <topic>" >&2
  exit 1
fi

KAFKA_BIN_DIR="$1"
BROKERS="$2"
TOPIC="$3"

if [ ! -x "$KAFKA_BIN_DIR/kafka-console-producer.sh" ]; then
  echo "kafka-console-producer.sh not found or not executable in $KAFKA_BIN_DIR" >&2
  exit 1
fi

# Ensure Python 3 is available
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH" >&2
  exit 1
fi

# Run the generator and pipe to Kafka producer
python3 "$(dirname "$0")/../sample_web_log.py" | "$KAFKA_BIN_DIR"/kafka-console-producer.sh --broker-list "$BROKERS" --topic "$TOPIC"
