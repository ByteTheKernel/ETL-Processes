from confluent_kafka import Producer

p = Producer({
    'bootstrap.servers': 'rc1b-7st62tq90h9le5nt.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'writer',
    'sasl.password': 'Sximy23Hve9dF-D',
    'ssl.ca.location': '/usr/local/share/ca-certificates/Yandex/YandexCA.crt',
})

messages = [
    '{"station_id":"iot-msk-001","sensor_type":"temperature","value":-2.5}',
    '{"station_id":"iot-spb-001","sensor_type":"wind_speed","value":12.6}',
    '{"station_id":"iot-ekb-001","sensor_type":"temperature","value":-8.4}',
]

for msg in messages:
    p.produce('sensors', msg.encode('utf-8'))
    print(f"Queued: {msg[:50]}")

p.flush()
print("Done!")
