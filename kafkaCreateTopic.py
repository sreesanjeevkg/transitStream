from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
    client_id='sreesanjeevkandasamygokulrajan'
)

topic_list = []
topic_list.append(NewTopic(name="checking_something"))
admin_client.create_topics(new_topics=topic_list, validate_only=False)

# Argument missing for parameter "self"