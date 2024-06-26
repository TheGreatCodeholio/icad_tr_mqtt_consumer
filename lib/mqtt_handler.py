#!/usr/bin/python3

import base64
import json
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import paho.mqtt.client as mqtt
import logging

from lib.call_processor import process_mqtt_call

module_logger = logging.getLogger('icad_tr_consumer.mqtt_client')


class MQTTClient:
    def __init__(self, global_config_data, num_workers=8):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.global_config_data = global_config_data
        self.broker_address = global_config_data.get("mqtt", {}).get("hostname", "")
        self.port = global_config_data.get("mqtt", {}).get("port", 1883)
        self.topic = global_config_data.get("mqtt", {}).get("topic", "trunk_recorder/feeds/audio")
        self.username = global_config_data.get("mqtt", {}).get("username", "")
        self.password = global_config_data.get("mqtt", {}).get("password", "")
        self.ca_certs = global_config_data.get("mqtt", {}).get("ca_certs", "")
        self.certfile = global_config_data.get("mqtt", {}).get("certfile", "")
        self.keyfile = global_config_data.get("mqtt", {}).get("keyfile", "")

        self.error_flag = threading.Event()

        self.message_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=num_workers)

        # Set the callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_disconnect = self.on_disconnect



    def on_connect(self, client, userdata, flags, reason_code, properties):
        if flags.session_present:
            if reason_code == 0:
                module_logger.info(f"MQTT - Connected to MQTT Broker Successfully")
            if reason_code > 0:
                module_logger.error(f"MQTT - Error Connection to Broker {reason_code}")
                self.error_flag.set()

        client.subscribe(self.topic)

    def on_subscribe(self, client, userdata, mid, reason_codes, properties):

        for index, sub_result in enumerate(reason_codes):
            status_code = reason_codes[index].getId(reason_codes[0].getName())
            module_logger.debug(sub_result)
            if status_code == 0:
                module_logger.info(f"Successfully Subscribed to {self.topic}")
            if status_code >= 128:
                module_logger.error(f"Error Subscribing to Topic {self.topic}: {sub_result}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        module_logger.info(f"MQTT - Disconnected from Broker: {reason_code}")
        self.error_flag.set()
        self.disconnect()

    def on_message(self, client, userdata, msg):
        module_logger.debug("Received Message, queuing for processing.")
        self.message_queue.put(msg)

    def process_messages(self):
        while not self.error_flag.is_set():  # Check if there's an error flag set to stop processing
            #module_logger.debug(f"MQTT - Waiting for {self.message_queue.qsize()} messages")
            try:
                msg = self.message_queue.get(timeout=1)  # Wait for a message with a timeout
                if msg is not None:
                    # Process the message using a thread pool and handle it completely before marking as done
                    future = self.executor.submit(self.process_message, msg)
                    future.add_done_callback(lambda f: self.message_queue.task_done())
            except queue.Empty:
                continue  # Continue if no message is available within the timeout period
            except Exception as e:
                module_logger.error(f"Error in processing messages: {e}")
                self.message_queue.task_done()

        module_logger.debug(f"MQTT - Exiting Queue Process Loop: {self.message_queue.qsize()}")

    def process_message(self, msg):
        try:
            module_logger.debug("Processing message from queue.")
            start_time = time.time()

            # Load call data and audio from MQTT
            data = json.loads(msg.payload)
            call_data = data.get("call", {})
            wav_data = base64.b64decode(call_data.get("audio_wav_base64", ""))
            metadata = call_data.get("metadata", {})

            wav_size = len(wav_data)
            wav_size_kb = wav_size / 1024

            module_logger.info(f"MQTT - New Message received from {msg.topic}")
            module_logger.debug(f"Payload size: {len(msg.payload)} bytes")
            module_logger.debug(f"Decoded WAV data size: {wav_size} bytes ({wav_size_kb:.2f} KB)")
            module_logger.debug(f"Message Metadata: {metadata}")

            # Process the call data
            process_mqtt_call(self.global_config_data, wav_data, metadata)

            process_time = time.time() - start_time
            module_logger.info(f"Message Processing Complete")
            module_logger.debug(f"Processing MQTT Message Took {round(process_time, 2)} seconds.")
        except Exception as e:
            module_logger.error(f"Error processing message: {e}")

    def start_mqtt_connection(self):
        module_logger.info("Connect")
        if not self.broker_address:
            module_logger.error(f"No MQTT Broker Address provided")

        try:
            if self.ca_certs and self.certfile and self.keyfile:
                self.client.tls_set(ca_certs=self.ca_certs, certfile=self.certfile, keyfile=self.keyfile)
                module_logger.info("Connecting to MQTT with mTLS auth.")

            elif self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
                module_logger.info(f"Connecting to MQTT with user auth: {self.username}")

            else:
                module_logger.warning("Connecting to MQTT without any authentication - not recommended!")

            self.client.connect(self.broker_address, self.port, 60)

        except Exception as e:
            module_logger.error(f"An unexpected error occurred while connecting to MQTT: {e}")
            self.error_flag.set()
            self.disconnect()

        try:
            # Start processing messages
            self.executor.submit(self.process_messages)
            # Start the loop to process received messages
            self.client.loop_start()
        except Exception as e:
            module_logger.error(f"An unexpected error occurred while running consumer. Exiting")
            self.error_flag.set()
            self.disconnect()

    def disconnect(self):
        module_logger.info("Disconnecting from MQTT Broker and shutting down all threads.")
        self.client.loop_stop()  # Stop the network loop
        self.client.disconnect()  # Disconnect the MQTT client
        self.executor.shutdown(wait=True)  # Shutdown the executor, waiting for tasks to complete

