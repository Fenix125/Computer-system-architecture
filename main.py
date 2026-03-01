import hazelcast

client = hazelcast.HazelcastClient(
    cluster_name="test-hazelcast",
    cluster_members=[
        "127.0.0.1:5701",
        "127.0.0.1:5702",
        "127.0.0.1:5703",
    ],
)

test_map = client.get_map("test").blocking()
test_map.set("key", "value")
print(test_map.get("key"))

client.shutdown()
