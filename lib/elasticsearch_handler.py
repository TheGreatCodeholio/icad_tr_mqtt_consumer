import logging
from elasticsearch import Elasticsearch, exceptions

module_logger = logging.getLogger('icad_tr_consumer.elastic_search')


class ElasticSearchClient:
    def __init__(self, config_data):
        self.config = config_data
        self.transmission_index = "icad-transmissions"
        self.rate_index = "icad-rates"
        self.recorder_index = "icad-recorders"
        self.duplicate_index = "icad-duplicates"

        self.client = Elasticsearch(
            hosts=[self.config["url"]],
            ca_certs=self.config.get("ca_cert", None),
            basic_auth=(self.config['username'], self.config['password'])
        )

        self.create_indices()

    def create_indices(self):
        self.create_rates_index()
        self.create_transmission_index()
        self.create_recorder_index()
        self.create_duplicates_index()

    def create_index_if_not_exists(self, index_name, mapping):
        try:
            module_logger.info(f"Checking if index {index_name} exists")
            if not self.client.indices.exists(index=index_name):
                module_logger.info(f"Index {index_name} does not exist. Creating index.")
                self.client.indices.create(index=index_name, body=mapping)
                module_logger.info(f"Index {index_name} created successfully.")
            else:
                module_logger.info(f"Index {index_name} already exists.")
        except exceptions.RequestError as e:
            module_logger.error(f"RequestError while creating index {index_name}: {e.info}")
        except exceptions.BadRequestError as e:
            module_logger.error(f"BadRequestError while creating index {index_name}: {e.info}")
        except exceptions.TransportError as e:
            module_logger.error(f"TransportError while creating index {index_name}: {e.info}")
        except Exception as e:
            module_logger.error(f"Error creating index {index_name}: {str(e)}")

    def create_transmission_index(self):
        mapping = {
            "mappings": {
                "dynamic": "false",
                "properties": {
                    "instance_id": {"type": "keyword"},
                    "audio_type": {"type": "keyword"},
                    "audio_wav_url": {"type": "text"},
                    "audio_m4a_url": {"type": "text"},
                    "audio_mp3_url": {"type": "text"},
                    "call_length": {"type": "integer"},
                    "duplex": {"type": "integer"},
                    "emergency": {"type": "integer"},
                    "encrypted": {"type": "integer"},
                    "freq": {"type": "long"},
                    "freqList": {
                        "type": "nested",
                        "properties": {
                            "error_count": {"type": "integer"},
                            "freq": {"type": "long"},
                            "len": {"type": "float"},
                            "pos": {"type": "float"},
                            "spike_count": {"type": "integer"},
                            "time": {"type": "date"}
                        }
                    },
                    "freq_error": {"type": "integer"},
                    "mode": {"type": "integer"},
                    "noise": {"type": "integer"},
                    "phase2_tdma": {"type": "integer"},
                    "priority": {"type": "integer"},
                    "recorder_num": {"type": "integer"},
                    "short_name": {"type": "keyword"},
                    "signal": {"type": "integer"},
                    "source_num": {"type": "integer"},
                    "srcList": {
                        "type": "nested",
                        "properties": {
                            "emergency": {"type": "integer"},
                            "pos": {"type": "integer"},
                            "signal_system": {"type": "keyword"},
                            "src": {"type": "integer"},
                            "tag": {"type": "text"},
                            "time": {
                                "type": "date",
                                "format": "strict_date_optional_time||epoch_second"
                            }
                        }
                    },
                    "start_time": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_second"
                    },
                    "stop_time": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_second"
                    },
                    "talkgroup": {"type": "integer"},
                    "talkgroup_alpha_tag": {"type": "text"},
                    "talkgroup_description": {"type": "text"},
                    "talkgroup_group": {"type": "text"},
                    "talkgroup_group_tag": {"type": "text"},
                    "talkgroup_tag": {"type": "text"},
                    "tdma_slot": {"type": "integer"},
                    "timestamp": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_second"
                    },
                    "tones": {
                        "type": "nested",
                        "properties": {
                            "hi_low_tone": {
                                "type": "nested",
                                "properties": {
                                    "alternations": {"type": "integer"},
                                    "detected": {"type": "float"},
                                    "end": {"type": "float"},
                                    "length": {"type": "float"},
                                    "start": {"type": "float"},
                                    "tone_id": {"type": "keyword"}
                                }
                            },
                            "long_tone": {
                                "type": "nested",
                                "properties": {
                                    "detected": {"type": "float"},
                                    "end": {"type": "float"},
                                    "length": {"type": "float"},
                                    "start": {"type": "float"},
                                    "tone_id": {"type": "keyword"}
                                }
                            },
                            "two_tone": {
                                "type": "nested",
                                "properties": {
                                    "detected": {"type": "float"},
                                    "end": {"type": "float"},
                                    "start": {"type": "float"},
                                    "tone_a_length": {"type": "float"},
                                    "tone_b_length": {"type": "float"},
                                    "tone_id": {"type": "keyword"}
                                }
                            }
                        }
                    },
                    "transcript": {
                        "type": "nested",
                        "properties": {
                            "addresses": {"type": "text"},
                            "process_time_seconds": {"type": "float"},
                            "segments": {
                                "type": "nested",
                                "properties": {
                                    "end": {"type": "float"},
                                    "segement_id": {"type": "integer"},
                                    "start": {"type": "float"},
                                    "text": {"type": "text"},
                                    "unit_tag": {"type": "text"},
                                    "words": {"type": "text"}
                                }
                            },
                            "transcript": {"type": "text"}
                        }
                    }
                }
            }
        }

        self.create_index_if_not_exists(self.transmission_index, mapping)

    def create_recorder_index(self):
        mapping = {
            "mappings": {
                "dynamic": "false",
                "properties": {
                    "instance_id": {"type": "keyword"},
                    "active_count": {"type": "integer"},
                    "available_count": {"type": "integer"},
                    "idle_count": {"type": "integer"},
                    "recording_count": {"type": "integer"},
                    "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_second"},
                }
            }
        }

        self.create_index_if_not_exists(self.recorder_index, mapping)

    def create_rates_index(self):
        mapping = {
            "mappings": {
                "dynamic": "false",
                "properties": {
                    "instance_id": {"type": "keyword"},
                    "decode_rate": {"type": "float"},
                    "short_name": {"type": "keyword"},
                    "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_second"},
                }
            }
        }
        self.create_index_if_not_exists(self.rate_index, mapping)

    def create_duplicates_index(self):
        mapping = {
            "mappings": {
                "dynamic": "false",
                "properties": {
                    "instance_id": {"type": "keyword"},
                    "talkgroup": {"type": "integer"},
                    "talkgroup_alpha_tag": {"type": "text"},
                    "talkgroup_description": {"type": "text"},
                    "talkgroup_group": {"type": "text"},
                    "talkgroup_group_tag": {"type": "text"},
                    "short_name": {"type": "keyword"},
                    "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_second"},
                }
            }
        }
        self.create_index_if_not_exists(self.duplicate_index, mapping)

    def index_document(self, index_type, document):
        try:
            if index_type == self.transmission_index:
                index_name = self.transmission_index
            elif index_type == self.recorder_index:
                index_name = self.recorder_index
            elif index_type == self.rate_index:
                index_name = self.rate_index
            elif index_type == self.duplicate_index:
                index_name = self.duplicate_index
            else:
                module_logger.warning(f"Unknown index type {index_type}")
                return

            response = self.client.index(
                index=index_name,
                body=document
            )
            module_logger.info(f"Document indexed successfully: {index_name}\n{document}")
        except Exception as e:
            module_logger.info(f"Error indexing document: {e}")
