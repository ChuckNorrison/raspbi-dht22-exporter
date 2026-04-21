#!/usr/bin/env python
"""
Prometheus DHT22 Exporter - Clean & configurable version
"""

import time
import argparse
import logging
import socket
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server
import Adafruit_DHT

# Configuration
SENSOR = Adafruit_DHT.DHT22
LOGFORMAT = "%(asctime)s - %(levelname)s [%(name)s] %(message)s"


class DHT22Collector:
    def __init__(self, node=None, pin=None, retries=10):
        self.node = node or socket.gethostname()
        self.pin = pin
        self.retries = retries
        self.logger = logging.getLogger("dht22_collector")
        self.last_read_time = 0.0

    def collect(self):
        temperature = None
        humidity = None
        now = time.time()

        try:
            # Respect minimum 2 seconds between sensor reads
            if now - self.last_read_time >= 2.0:
                humidity, temperature = Adafruit_DHT.read_retry(
                    SENSOR, self.pin, retries=self.retries, delay_seconds=0.5
                )

                if humidity is None or temperature is None:
                    raise RuntimeError("Sensor returned None values")

                # Sanity check
                if not (0 <= humidity <= 100) or not (-50 <= temperature <= 80):
                    raise ValueError(f"Implausible values: {temperature}°C, {humidity}%")

                self.last_read_time = now
                self.logger.debug("Read successful → %.1f°C / %.1f%%", temperature, humidity)

        except Exception as e:
            self.logger.warning("Failed to read DHT22: %s", e)
            temperature = float("nan")
            humidity = float("nan")

        # Temperature metric
        yield GaugeMetricFamily(
            "temperature_in_celsius",
            "Temperature measured by DHT22 sensor",
            labels=["node"]
        ).add_metric([self.node], temperature)

        # Humidity metric
        yield GaugeMetricFamily(
            "humidity_in_percent",
            "Relative humidity measured by DHT22 sensor",
            labels=["node"]
        ).add_metric([self.node], humidity)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHT22 Prometheus Exporter")
    parser.add_argument("-n", "--node", type=str, default=socket.gethostname(),
                        help="Node name (default: hostname)")
    parser.add_argument("-p", "--port", type=int, default=9123,
                        help="HTTP port for Prometheus scraping")
    parser.add_argument("-i", "--interval", type=int, default=60,
                        help="Main loop sleep interval in seconds (affects CPU usage only)")
    parser.add_argument("-r", "--retries", type=int, default=15,
                        help="Number of read retries for Adafruit_DHT")
    parser.add_argument("-g", "--gpiopin", type=int, default=4,
                        help="GPIO pin (BCM numbering)")
    parser.add_argument("-l", "--loglevel", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO", help="Logging level")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=getattr(logging, args.loglevel), format=LOGFORMAT)
    logger = logging.getLogger("dht22_exporter")

    logger.info("Starting DHT22 exporter on port %s (Pin %s, Node=%s, Interval=%ds)",
                args.port, args.gpiopin, args.node, args.interval)

    try:
        start_http_server(args.port)
        REGISTRY.register(DHT22Collector(args.node, args.gpiopin, args.retries))

        logger.info("Exporter is ready and available for scraping immediately.")

        # Main loop
        while True:
            logger.debug("Sleeping for %d seconds", args.interval)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Exporter stopped by user.")
    except Exception as e:
        logger.error("Unexpected error in main loop", exc_info=True)
        raise
