"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen
import asyncio



logger = logging.getLogger(__name__)



class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
         "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "group.id": "group_1",
            "auto.offset.reset": self.offset_earliest
        }

        #  Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            
        subscribed_topic = ["weather_events", "StationTurnstile", "transformedStation", "com.udacity.station.arrivals"]
           
        self.consumer.subscribe( subscribed_topic, on_assign=on_assign )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # set offset to beginning
        logger.info("on_assign is completing successfully")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
            #partition.self.offset_earliest = True
            
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
       
        while True:
        message = self.consumer.poll(self.consume_timeout)
        if message is None:
            print("no message received by consumer")
            return 0
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
            return 0
        else:
            print(f"consumed message {message.key()}: {message.value()}")
            return 1
        #await asyncio.sleep(self.sleep_secs)

        #logger.info("_consume is incomplete - skipping")
        #return 0


    def close(self):
        """Cleans up any open kafka consumers"""
         self.consumer.close()
