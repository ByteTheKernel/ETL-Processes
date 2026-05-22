from kafka import KafkaProducer
import json, ssl

context = ssl.create_default_context()
context.load_verify_locations('/home/vlado/certs/YandexInternalRootCA.crt')

producer = KafkaProducer(
    bootstrap_servers=['rc1b-0m5o10ck2c71qma0.mdb.yandexcloud.net:9091'],
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='kafka-user',
    sasl_plain_password='secret',
    ssl_context=context,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(5):
    msg = {"id": i, "text": f"HW15 message #{i}", "source": "pyspark-hw"}
    future = producer.send('hw15-topic', msg)
    result = future.get(timeout=10)
    print(f"Sent: {msg} → partition={result.partition}, offset={result.offset}")

producer.flush()
producer.close()
print("Done!")