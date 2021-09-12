"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
#  Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("cta.chicago.stations", key_type=str, value_type=Station, partitions=1)

out_topic = app.topic("cta.chicago.stations.transformed", partitions=1, value_type=TransformedStation)
#  Define a Faust Table
transformedStationTable = app.Table("TransformedStationTable",
  default=str,
  partitions=1,
  changelog_topic=out_topic,
)


@app.agent(topic)
async def station(stations):
    async for station in stations.group_by(Station.station_id):
        if station.red == True:
            line = "red"
        elif station.green == True:
            line = "green"
        elif station.blue == True:
            line = "blue"
            
        transformedStationTable[station.station_id] = TransformedStation(station_id = station.station_id, station_name = station.station_name, order = station.order, line = line)
        print(f" The station ID is: {transformedStationTable[station.station_id]}")
     #   print(f"The station id is {transformedStationTable[station.station_id]}, the station name is {transformedStationTable[station.station_name]}, the order is {transformedStationTable[station.order]} and the color is {transformedStationTable[station.color]}")


if __name__ == "__main__":
    app.main()
