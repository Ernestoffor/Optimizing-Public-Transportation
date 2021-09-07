"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        
        
        try:
            logger.info("weather process_message is processing")
            message_value = message.value()
            self.temperature = message_value.get("temperature")
            self.status = message_value.get("status")
            logger.info(f"The incoming weather status is: {self.status}")
            
        except Exception as e:
            logger.info("The weather process_message returns errors")
            logger.error(e)
        
       
