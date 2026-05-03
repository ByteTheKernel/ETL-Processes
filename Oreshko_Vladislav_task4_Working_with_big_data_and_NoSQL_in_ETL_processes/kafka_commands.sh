#!/usr/bin/env bash
# Task 4 - Webinar 12: Kafka commands via kcat (kafkacat)
# Author: Vladislav Oreshko
#
# kcat is the CLI tool used in the webinar demo.
# Install: sudo apt-get install kafkacat  (or brew install kcat on macOS)
#
# Replace the values below with your actual cluster connection details:
#   BROKER  – from Yandex Cloud Console → Managed Kafka → Hosts
#   USER    – Kafka user with producer/consumer role
#   PASS    – Kafka user password
#   CA_CERT – Yandex Internal Root CA (download once, path stays the same)

BROKER="rc1a-<your-broker-id>.mdb.yandexcloud.net:9091"
TOPIC="sensors"
USER="mkf-user"
PASS="<kafka-password>"
CA_CERT="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"

# Common SSL/SASL options (reused in both producer and consumer)
COMMON_OPTS="
  -X security.protocol=SASL_SSL
  -X sasl.mechanisms=SCRAM-SHA-512
  -X sasl.username=${USER}
  -X sasl.password=${PASS}
  -X ssl.ca.location=${CA_CERT}
  -Z
"

# ---------------------------------------------------------------------------
# PRODUCER – pipe sensors.json through jq into Kafka
#   -rc   : raw output, compact (one JSON object per line)
#   -P    : producer mode
#   -k key: use the literal string "key" as the Kafka message key
# ---------------------------------------------------------------------------
echo "=== Sending data/sensors.json to topic '${TOPIC}' ==="

jq -rc '.[]' data/sensors.json | kcat -P \
  -b "${BROKER}" \
  -t "${TOPIC}" \
  -k key \
  ${COMMON_OPTS}

echo "Messages sent."

# ---------------------------------------------------------------------------
# CONSUMER – read messages from the beginning and print with key separator
#   -C    : consumer mode
#   -K:   : print message key followed by ':' before the value
#   -e    : exit after all messages have been read (EOF)
# ---------------------------------------------------------------------------
echo ""
echo "=== Reading messages from topic '${TOPIC}' ==="

kcat -C \
  -b "${BROKER}" \
  -t "${TOPIC}" \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username="${USER}" \
  -X sasl.password="${PASS}" \
  -X ssl.ca.location="${CA_CERT}" \
  -Z \
  -K: \
  -e
