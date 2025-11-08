#!/usr/bin/env python3
"""
node1_producer_fixed.py
Improved and robust Node 1 producer script.
"""

import threading
import queue
import time
import csv
import logging
import os
import subprocess
import signal
import sys
from datetime import datetime

# MySQL client
import pymysql

# Try to import kafka-python, but give a helpful error if it's missing / conflicting package installed
try:
    from kafka import KafkaProducer
except Exception as e:
    sys.stderr.write(
        "\nERROR: Could not import kafka.KafkaProducer.\n"
        " - Make sure you have installed the correct package: `pip install kafka-python`\n"
        " - If you previously installed a package named `kafka`, uninstall it: `pip uninstall kafka -y`\n"
        f"Underlying import error: {e}\n\n"
    )
    raise

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s')
logger = logging.getLogger("node1_producer")

# ---------- Database helper ----------
class MySQLDatabase:
    def __init__(self, host='172.24.115.15', user='kafka_user', password='KafkaP@ss123!', database='streaming_system', port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def get_connection(self):
        return pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.password,
                               database=self.database,
                               port=self.port,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor,
                               autocommit=False)

    def get_approved_topics(self):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id, name FROM topics WHERE status = 'approved' AND is_created = 0")
                    rows = cursor.fetchall()
            return rows
        except Exception as e:
            logger.error("Error fetching topics: %s", e)
            return []

    def mark_topic_as_created(self, topic_id):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE topics SET is_created = 1, status = 'active' WHERE id = %s", (topic_id,))
                conn.commit()
        except Exception as e:
            logger.error("Error marking topic as created: %s", e)

# ---------- Topic watcher ----------
class TopicWatcherThread(threading.Thread):
    def __init__(self, kafka_path, kafka_bootstrap, db, poll_interval=5):
        super().__init__(daemon=True, name="TopicWatcherThread")
        self.kafka_path = kafka_path
        self.kafka_bootstrap = kafka_bootstrap
        self.db = db
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

    def run(self):
        logger.info("Topic watcher started.")
        while not self._stop_event.is_set():
            topics = self.db.get_approved_topics()
            for topic in topics:
                topic_name = topic['name']
                topic_id = topic['id']
                try:
                    if self._create_topic_in_kafka(topic_name):
                        self.db.mark_topic_as_created(topic_id)
                        logger.info("Topic created and marked active: %s", topic_name)
                    else:
                        logger.error("Failed to create topic in Kafka: %s", topic_name)
                except Exception as e:
                    logger.exception("Exception in topic creation loop: %s", e)
            self._stop_event.wait(self.poll_interval)

    def stop(self):
        self._stop_event.set()

    def _create_topic_in_kafka(self, topic_name):
        """
        Uses kafka-topics.sh to create the topic. This will return True on success.
        """
        kafka_topics_sh = os.path.join(self.kafka_path, "bin", "kafka-topics.sh")
        if not os.path.isfile(kafka_topics_sh):
            logger.error("kafka-topics.sh not found at %s", kafka_topics_sh)
            return False

        cmd = [
            kafka_topics_sh,
            "--create",
            "--topic", topic_name,
            "--bootstrap-server", self.kafka_bootstrap,
            "--partitions", "3",
            "--replication-factor", "1",
            "--if-not-exists"
        ]
        try:
            logger.debug("Creating topic with command: %s", " ".join(cmd))
            completed = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)
            if completed.returncode == 0:
                logger.info("kafka-topics.sh output: %s", completed.stdout.strip())
                return True
            else:
                logger.error("kafka-topics.sh failed (rc=%s). stdout=%s stderr=%s", completed.returncode, completed.stdout.strip(), completed.stderr.strip())
                return False
        except subprocess.TimeoutExpired:
            logger.error("kafka-topics.sh timed out for topic %s", topic_name)
            return False
        except Exception as e:
            logger.exception("Exception while creating topic: %s", e)
            return False

# ---------- Publisher ----------
class PublisherThread(threading.Thread):
    def __init__(self, message_queue, kafka_bootstrap):
        super().__init__(daemon=True, name="PublisherThread")
        self.message_queue = message_queue
        self.kafka_bootstrap = kafka_bootstrap
        self._stop_event = threading.Event()
        self.producer = self._create_producer()

    def _create_producer(self):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_bootstrap],
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else (k if k is None else str(k).encode('utf-8')),
                value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else (v if v is None else str(v).encode('utf-8')),
                retries=5,
                acks='all',
                linger_ms=5
            )
            logger.info("KafkaProducer created and connected to %s", self.kafka_bootstrap)
            return producer
        except Exception as e:
            logger.exception("Failed to create KafkaProducer: %s", e)
            raise

    def run(self):
        logger.info("Publisher thread started.")
        while not self._stop_event.is_set():
            try:
                topic, key, value = self.message_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                future = self.producer.send(topic, key=key, value=value)
                record_metadata = future.get(timeout=10)  # wait for send to finish
                logger.info("Published message to %s partition=%s offset=%s key=%s", record_metadata.topic, record_metadata.partition, record_metadata.offset, key)
            except Exception as e:
                logger.exception("Failed to publish message: %s", e)
            finally:
                try:
                    self.message_queue.task_done()
                except Exception:
                    pass

    def stop(self):
        self._stop_event.set()
        try:
            logger.info("Flushing and closing Kafka producer...")
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)
        except Exception as e:
            logger.exception("Error while closing producer: %s", e)

# ---------- Input listener ----------
class InputListenerThread(threading.Thread):
    def __init__(self, message_queue, db):
        super().__init__(daemon=True, name="InputListenerThread")
        self.message_queue = message_queue
        self.db = db
        self._stop_event = threading.Event()

    def run(self):
        logger.info("Input listener started. Type commands: send <topic> <key> <value>, csv <file>, topics, quit")
        while not self._stop_event.is_set():
            try:
                cmd_line = input(">> ").strip()
            except EOFError:
                logger.info("EOF received, stopping input listener.")
                self._stop_event.set()
                break
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received, stopping input listener.")
                self._stop_event.set()
                break
            except Exception as e:
                logger.exception("Error reading input: %s", e)
                continue

            if not cmd_line:
                continue

            parts = cmd_line.split(' ', 3)  # keeps the fourth element as the remainder (value)
            cmd = parts[0].lower()

            if cmd == "send":
                if len(parts) < 4:
                    print("Usage: send <topic> <key> <value>")
                    continue
                topic, key, value = parts[1], parts[2], parts[3]
                self.message_queue.put((topic, key, value))
            elif cmd == "csv":
                if len(parts) < 2:
                    print("Usage: csv <csv_file>")
                    continue
                csv_file = parts[1]
                self._load_csv(csv_file)
            elif cmd == "topics":
                self._print_topics()
            elif cmd == "quit":
                logger.info("Quit command received. Exiting.")
                os._exit(0)
            else:
                print("Unknown command. Commands: send, csv, topics, quit")

    def stop(self):
        self._stop_event.set()

    def _load_csv(self, csv_file):
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    topic = row.get('topic')
                    key = row.get('key')
                    value = row.get('value')
                    if topic and key and value:
                        self.message_queue.put((topic, key, value))
                        time.sleep(0.02)
            print(f"Loaded csv file '{csv_file}' and enqueued messages.")
        except FileNotFoundError:
            print(f"CSV file '{csv_file}' not found.")
        except Exception as e:
            logger.exception("Error loading CSV: %s", e)

    def _print_topics(self):
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT name, status FROM topics ORDER BY id")
                    rows = cursor.fetchall()
            print("Topics:")
            for row in rows:
                print(f"- {row['name']} [{row['status']}]")
        except Exception as e:
            logger.exception("Error fetching topics: %s", e)

# ---------- main / orchestrator ----------
def main():
    kafka_bootstrap = "172.24.115.15:9092"
    kafka_path = "/opt/kafka"   # adjust if your kafka installation path differs
    db = MySQLDatabase(host="172.24.115.15", user="kafka_user", password="KafkaP@ss123!", database="streaming_system")
    message_queue = queue.Queue(maxsize=10000)

    # Threads
    topic_watcher = TopicWatcherThread(kafka_path, kafka_bootstrap, db)
    publisher = PublisherThread(message_queue, kafka_bootstrap)
    input_listener = InputListenerThread(message_queue, db)

    # Graceful stop helper
    def shutdown(signum, frame):
        logger.info("Shutdown signal received (%s). Stopping threads...", signum)
        try:
            topic_watcher.stop()
            publisher.stop()
            input_listener.stop()
        except Exception:
            pass
        # allow some time for threads to finish
        time.sleep(1.0)
        logger.info("Exiting.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # start
    topic_watcher.start()
    publisher.start()
    input_listener.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    main()
