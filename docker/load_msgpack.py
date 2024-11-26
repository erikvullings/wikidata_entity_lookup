import msgpack
import redis

# Connect to KeyDB
r = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

# Load MessagePack data
with open("/data/kv_data.msgpack", "rb") as f:
    unpacker = msgpack.Unpacker(f, raw=False)
    for item in unpacker:
        key = item["id"]  # Extract key (adjust as per your structure)
        value = msgpack.packb(item)  # Store as MessagePack value
        r.set(key, value)

print("All key-value pairs loaded into KeyDB!")
