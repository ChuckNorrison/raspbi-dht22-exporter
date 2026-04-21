#!/usr/bin/env python
"""
Prometheus DHT22 Exporter with improved error handling
"""

import time
import argparse
import logging
import socket
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from prometheus_client import start_http_server
import Adafruit_DHT

# Configuration
SENSOR = Adafruit_DHT.DHT22
LOGFORMAT = "%(asctime)s - %(levelname)s [%(name)s] %(threadName)s %(message)s"

class CustomCollector:
    """Custom Prometheus collector with robust error handling"""

    def __init__(self, node=None, pin=None, retries=10):
        self.node = node or socket.gethostname()
        self.pin = pin
        self.retries = retries
        self.logger = logging.getLogger("dht22_collector")

    def collect(self):
        """Collect metrics with comprehensive error handling"""
        temperature = None
        humidity = None

        try:
            # Read with retries (Adafruit's built-in retry)
            humidity, temperature = Adafruit_DHT.read_retry(
                SENSOR, self.pin, retries=self.retries, delay_seconds=0.5
            )

            if humidity is None or temperature is None:
                raise RuntimeError("Sensor returned None values")

            # Basic sanity checks
            if not (0 <= humidity <= 100) or not (-50 <= temperature <= 80):
                raise ValueError(f"Implausible values: temp={temperature}, hum={humidity}")

            self.logger.debug(
                "Successfully read - Temp: %.1f°C, Humidity: %.1f%%", temperature, humidity
            )

        except Exception as e:  # Catch all sensor-related errors
            self.logger.warning(
                "Failed to read DHT22: %s", str(e)
            )

            # Return last known good values or NaN-like behavior (Prometheus handles None)
            temperature = temperature if temperature is not None else float("nan")
            humidity = humidity if humidity is not None else float("nan")

        # === Expose sensor metrics ===
        temp_gauge = GaugeMetricFamily(
            "temperature_in_celsius",
            "Temperature in Celsius",
            labels=["node"]
        )
        temp_gauge.add_metric([self.node], temperature)
        yield temp_gauge

        hum_gauge = GaugeMetricFamily(
            "humidity_in_percent",
            "Relative humidity in percent",
            labels=["node"]
        )
        hum_gauge.add_metric([self.node], humidity)
        yield hum_gauge

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prometheus DHT22 sensor exporter")
    parser.add_argument("-n", "--node", type=str, default=socket.gethostname(),
                        help="Node name (default: hostname)")
    parser.add_argument("-p", "--port", type=int, default=9123,
                        help="HTTP port for Prometheus scraping")
    parser.add_argument("-i", "--interval", type=int, default=120,
                        help="Sleep interval between collections (seconds)")
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

    logger.info("Starting DHT22 exporter on port %s (pin %s, node=%s)",
                args.port, args.gpiopin, args.node)

    try:
        start_http_server(args.port)
        REGISTRY.register(CustomCollector(args.node, args.gpiopin, args.retries))

        logger.info("Exporter ready - waiting for Prometheus scrapes")

        while True:
            logger.debug("Sleeping for %s seconds", args.interval)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error("Unexpected error in main loop: %s", e, exc_info=True)
        raise
