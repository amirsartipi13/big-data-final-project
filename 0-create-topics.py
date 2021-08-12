from kafka.admin import KafkaAdminClient, NewTopic


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
)
topics = ['pre-process', 'persistence', 'channel-history', 'statistics']
topic_list = []
for topic in topics:
    topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    
admin_client.create_topics(new_topics=topic_list, validate_only=False)