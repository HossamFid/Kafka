from confluent_kafka.admin import NewTopic, AdminClient, KafkaException
import logging


conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
}
adminClient = AdminClient(conf)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


me = 'HossamFid-01'
meError = me + '-error-topic'
numPartitions = 4
replicationFactor = 1


newTopics = [
    NewTopic(me, num_partitions=numPartitions, replication_factor=replicationFactor),
    NewTopic(meError, num_partitions=numPartitions, replication_factor=replicationFactor)
]


try:
    logger.info(f"Creating topics: {[t.topic for t in newTopics]}")
    futureTopics = adminClient.create_topics(newTopics)

   
    for topic, future in futureTopics.items():
        try:
            future.result()  
            logger.info(f"Topic '{topic}' created successfully.")
        except KafkaException as e:
            logger.error(f"Failed to create topic '{topic}': {e}")

except KafkaException as e:
    logger.error(f"An error occurred during topic creation: {e}")

try:
    currentTopics = adminClient.list_topics(timeout=10).topics
    existingTopics = set(currentTopics.keys())
    logger.info(f"Existing topics: {existingTopics}")

except KafkaException as e:
    logger.error(f"Failed to list topics: {e}")
