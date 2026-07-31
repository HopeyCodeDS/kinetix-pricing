import json
from kafka import KafkaConsumer

print("Connecting to Kafka (5-second timeout)...")

consumer = KafkaConsumer(
    'dbserver1.public.orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,  # <--- CRITICAL: Stops hanging after 5 seconds
    enable_auto_commit=False
)

count = 0
for message in consumer:
    count += 1
    try:
        # Manually decode and parse the JSON
        raw_value = message.value.decode('utf-8')
        data = json.loads(raw_value)
        
        payload = data.get('payload', {})
        op = payload.get('op')
        after = payload.get('after', {})
        
        print(f"Message {count}: op='{op}', order_id={after.get('order_id')}, quantity={after.get('quantity')}")
    except Exception as e:
        print(f"Error parsing message: {e}")

consumer.close()

if count == 0:
    print("⚠️ No messages found in the topic within 5 seconds.")
else:
    print("✅ Kafka read test complete!")