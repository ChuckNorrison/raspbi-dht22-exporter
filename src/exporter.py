#!/usr/bin/env python
"""
Prometheus DHT22 Exporter - Interval controls sensor polling rate
"""

import time
import argparse
import logging
import socket
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server
import Adafruit_DHT
import threading

# Configuration
SENSOR = Adafruit_DHT.DHT22
LOGFORMAT = "%(asctime)s - %(levelname)s [%(name)s] %(message)s"


class DHT22Collector:
    def __init__(self, node=None, pin=None, retries=15, interval=60):
        self.node = node or socket.gethostname()
        self.pin = pin
        self.retries = retries
        self.interval = interval
        self.logger = logging.getLogger("dht22_collector")
        
        # Cached values
        self.temperature = float("nan")
        self.humidity = float("nan")
        self.last_read_time = 0.0
        self.lock = threading.Lock()

    def read_sensor(self):
        """Read sensor in background thread"""
        now = time.time()

        if self.last_read_time == 0.0:
            self.last_read_time = now + self.interval
            self.logger.info("First sensor read scheduled in %d seconds", self.interval)
            return
        
        if now - self.last_read_time < self.interval:
            return  # too early

        try:
            humidity, temperature = Adafruit_DHT.read_retry(
                SENSOR, self.pin, retries=self.retries, delay_seconds=0.5
            )

            if humidity is None or temperature is None:
                raise RuntimeError("Sensor returned None values")

            # Sanity check
            if not (0 <= humidity <= 100) or not (-50 <= temperature <= 80):
                raise ValueError(f"Implausible values: {temperature}°C, {humidity}%")

            with self.lock:
                self.temperature = temperature
                self.humidity = humidity
                self.last_read_time = now

            self.logger.debug("Read successful → %.1f°C / %.1f%%", temperature, humidity)

        except Exception as e:
            self.logger.warning("Failed to read DHT22: %s", e)
            # Keep last known good values on failure

    def collect(self):
        """Return cached values instantly, called by prometheus scraper"""
        self.logger.debug("collect() triggered by Prometheus scrape request")
        
        with self.lock:
            temp = self.temperature
            hum = self.humidity

        # Temperature
        temp_gauge = GaugeMetricFamily(
            "temperature_in_celsius",
            "Temperature measured by DHT22 sensor",
            labels=["node"]
        )
        temp_gauge.add_metric([self.node], temp)
        yield temp_gauge

        # Humidity
        hum_gauge = GaugeMetricFamily(
            "humidity_in_percent",
            "Relative humidity measured by DHT22 sensor",
            labels=["node"]
        )
        hum_gauge.add_metric([self.node], hum)
        yield hum_gauge


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHT22 Prometheus Exporter")
    parser.add_argument("-n", "--node", type=str, default=socket.gethostname(),
                        help="Node name (default: hostname)")
    parser.add_argument("-p", "--port", type=int, default=9123,
                        help="HTTP port for Prometheus scraping")
    parser.add_argument("-i", "--interval", type=int, default=60,
                        help="Sensor polling interval in seconds (affects battery/sensor lifetime)")
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

    logger.info("Starting DHT22 exporter on port %s (Pin %s, Node=%s, Poll interval=%ds)",
                args.port, args.gpiopin, args.node, args.interval)

    collector = DHT22Collector(
        node=args.node,
        pin=args.gpiopin,
        retries=args.retries,
        interval=args.interval
    )

    try:
        start_http_server(args.port)
        REGISTRY.register(collector)

        logger.info("Exporter ready - sensor will be polled every %d seconds", args.interval)

        # Background sensor reading loop
        while True:
            collector.read_sensor()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Exporter stopped by user.")
    except Exception as e:
        logger.error("Unexpected error in main loop", exc_info=True)
        raise
