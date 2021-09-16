"""Producer base-class providing common utilites and functionality"""
import logging
import time
import asyncio

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below.
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

       # Configure the AvroProducer
        self.producer = AvroProducer(
             self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema
             )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client= AdminClient({"bootstrap.servers": BROKER_URL})
        
        
        # config={"cleanup.policy":"delete",
                 #   "compression.type":"lz4",
                  #  "delete.retention.ms":2000,
                   #  "file.delete.delay.ms":2000,
                    #        }
        
        futures = client.create_topics(
             [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas
                )
            ]
         )
        for topic, future in futures.items():
            try:
                future.result()
                print(f"{topic} topic created")
                
            except Exception as e:
                print(f"failed to create topic {topic}: {e}")
               # logger.info("topic creation kafka integration incomplete - skipping")
                #raise
       # logger.info("topic creation kafka integration completed  - successfully")
    
    

    
    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        
        
        logger.info("producer closing gracefully")

        self.producer.flush()
        
    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
    
    
    
