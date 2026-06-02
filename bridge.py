#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modbus naar MQTT Bridge voor USR-N580 Gateway & Home Assistant Autodiscovery.
Geschikt voor 24/7 werking op een Raspberry Pi 3B (CLI-only).
"""

import os
import sys
import time
import yaml
import signal
import argparse
import json
import logging
import threading
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType
import paho.mqtt.client as mqtt

# ==============================================================================
# Logging Configuratie
# ==============================================================================
class EmojiFormatter(logging.Formatter):
    """Aangepaste formatter om logs visueel aantrekkelijk te maken."""
    def format(self, record):
        level = record.levelno
        if level >= logging.ERROR:
            emoji = "❌ "
        elif level >= logging.WARNING:
            emoji = "⚠️ "
        elif record.name == "mqtt":
            emoji = "📡 "
        elif record.name == "modbus":
            emoji = "📖 "
        else:
            emoji = "ℹ️ "
        
        # Voeg emoji toe aan het begin van het bericht
        record.msg = f"{emoji}{record.msg}"
        return super().format(record)

# Setup logger
logger = logging.getLogger("bridge")
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(EmojiFormatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

# Aparte sub-loggers voor details
modbus_logger = logging.getLogger("modbus")
modbus_logger.addHandler(log_handler)
modbus_logger.propagate = False

mqtt_logger = logging.getLogger("mqtt")
mqtt_logger.addHandler(log_handler)
mqtt_logger.propagate = False

# ==============================================================================
# Bridge Daemon Klasse
# ==============================================================================
class ModbusMqttBridge:
    def __init__(self, config_path, debug=False, dry_run=False):
        self.config_path = config_path
        self.debug = debug
        self.dry_run = dry_run
        self.running = True
        self.config = {}
        self.modbus_client = None
        self.mqtt_client = None
        self.mqtt_connected = False
        self.slave_states = {}  # Track connectivity status per slave
        self.last_published_values = {}  # Track last published values per sensor
        self.is_addon = False
        
        # Statistieken & status tracking voor dashboard
        self.start_time = time.time()
        self.stats = {"total_polls": 0, "successful_reads": 0, "failed_reads": 0}
        self.sensor_values = {}  # unique_id -> {"value": val, "timestamp": str}
        self.mqtt_buffer = {}  # topic -> (payload, retain)
        self.buffer_lock = threading.Lock()

        if self.debug:
            logger.setLevel(logging.DEBUG)
            modbus_logger.setLevel(logging.DEBUG)
            mqtt_logger.setLevel(logging.DEBUG)
            # Schakel pymodbus interne debug logging in
            logging.getLogger("pymodbus").setLevel(logging.DEBUG)
        else:
            modbus_logger.setLevel(logging.INFO)
            mqtt_logger.setLevel(logging.INFO)
            logging.getLogger("pymodbus").setLevel(logging.WARNING)

        # Inlezen van configuratie
        self.load_config()
        
        # Start de status HTTP Ingress server
        self.start_http_server()

    def get_device_info_by_slave(self, slave):
        """Bepaal apparaatnaam en identifiers op basis van slave ID."""
        if slave == 1:
            dev_name = "Warmtepomp"
            dev_id = "warmtepomp"
        elif slave == 2:
            dev_name = "Sinotimer"
            dev_id = "sinotimer"
        elif slave in (3, 4, 5):
            dev_name = f"KWS Meter {slave - 2}"
            dev_id = f"kws_meter_{slave - 2}"
        elif 51 <= slave <= 55:
            dev_name = f"Growatt Inverter {slave}"
            dev_id = f"growatt_inverter_{slave}"
        else:
            dev_name = f"Modbus Apparaat {slave}"
            dev_id = f"modbus_device_{slave}"
        return dev_name, dev_id

    def load_config(self):
        """Laad het YAML configuratiebestand of de Home Assistant options.json in."""
        if os.path.exists("/data/options.json"):
            logger.info("Home Assistant Addon gedetecteerd. Opties inlezen uit /data/options.json...")
            try:
                with open("/data/options.json", 'r', encoding='utf-8') as f:
                    ha_options = json.load(f)
                
                # Transformeer de platte HA-addon opties naar de interne geneste structuur
                self.config = {
                    "mqtt": {
                        "broker": ha_options.get("mqtt_broker", "localhost"),
                        "port": ha_options.get("mqtt_port", 1883),
                        "username": ha_options.get("mqtt_username"),
                        "password": ha_options.get("mqtt_password"),
                        "topic_prefix": ha_options.get("mqtt_topic_prefix", "usr_n580_bridge"),
                        "discovery_prefix": ha_options.get("mqtt_discovery_prefix", "homeassistant"),
                        "client_id": "modbus_mqtt_bridge",
                        "keepalive": 60
                    },
                    "modbus": {
                        "host": ha_options.get("modbus_host", "192.168.50.96"),
                        "port": ha_options.get("modbus_port", 41),
                        "timeout": 3,
                        "poll_interval": ha_options.get("poll_interval", 15)
                    },
                    "log_level": ha_options.get("log_level", "info"),
                    "sensors": []
                }
                
                # Probeer sensors uit de lokale config.yaml te laden indien aanwezig, anders uit ha_options
                if os.path.exists("config.yaml"):
                    logger.info("Lokale config.yaml gevonden. Laden van sensors uit config.yaml...")
                    try:
                        with open("config.yaml", 'r', encoding='utf-8') as cf:
                            local_cfg = yaml.safe_load(cf)
                            self.config["sensors"] = local_cfg.get("sensors", [])
                        logger.info(f"Sensoren succesvol geladen uit config.yaml (Totaal: {len(self.config['sensors'])} sensoren).")
                    except Exception as e:
                        logger.error(f"Fout bij laden van sensors uit config.yaml: {e}. Terugvallen op addon opties...")
                        self.config["sensors"] = ha_options.get("sensors", [])
                else:
                    logger.warning("Geen lokale config.yaml gevonden. Laden van sensors uit addon opties...")
                    self.config["sensors"] = ha_options.get("sensors", [])

                self.is_addon = True
                logger.info("Addon opties succesvol ingeladen en getransformeerd!")
            except Exception as e:
                logger.error(f"Fout bij inlezen Home Assistant opties: {e}")
                sys.exit(1)
        else:
            logger.info(f"Configuratie inlezen van: {self.config_path}")
            if not os.path.exists(self.config_path):
                logger.error(f"Configuratiebestand niet gevonden op: {self.config_path}")
                sys.exit(1)
            
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f)
                logger.info("Configuratie succesvol ingeladen!")
            except Exception as e:
                logger.error(f"Fout bij parsen van YAML: {e}")
                sys.exit(1)

        # Configureer loggers gebaseerd op log_level uit de config, tenzij debug op CLI is meegegeven
        if not self.debug:
            log_level_str = str(self.config.get("log_level", "info")).upper()
            numeric_level = getattr(logging, log_level_str, logging.INFO)
            
            logger.setLevel(numeric_level)
            modbus_logger.setLevel(numeric_level)
            mqtt_logger.setLevel(numeric_level)
            
            # Pymodbus debug is erg luidruchtig, toon dat alleen bij DEBUG
            if numeric_level == logging.DEBUG:
                logging.getLogger("pymodbus").setLevel(logging.DEBUG)
            else:
                logging.getLogger("pymodbus").setLevel(logging.WARNING)
            
            logger.info(f"Log-level ingesteld op: {log_level_str}")

    # --------------------------------------------------------------------------
    # MQTT Functionaliteiten
    # --------------------------------------------------------------------------
    def setup_mqtt(self):
        """Configureer de MQTT verbinding."""
        if self.dry_run:
            logger.info("[DRY-RUN] MQTT Setup overgeslagen.")
            return

        mqtt_cfg = self.config.get("mqtt", {})
        broker = mqtt_cfg.get("broker", "localhost")
        port = mqtt_cfg.get("port", 1883)
        username = mqtt_cfg.get("username")
        password = mqtt_cfg.get("password")
        client_id = mqtt_cfg.get("client_id", "modbus_mqtt_bridge")
        keepalive = mqtt_cfg.get("keepalive", 60)

        # Suffix client_id for developer isolation if not running as addon
        if not getattr(self, "is_addon", False):
            import socket
            client_id = f"{client_id}_dev_{socket.gethostname()}"

        mqtt_logger.info(f"MQTT Client initialiseren (ID: {client_id})...")
        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )

        if username and password:
            self.mqtt_client.username_pw_set(username, password)

        # callbacks koppelen
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        mqtt_logger.info(f"Verbinden met MQTT broker op {broker}:{port}...")
        try:
            self.mqtt_client.connect(broker, port, keepalive)
            self.mqtt_client.loop_start()
        except Exception as e:
            mqtt_logger.error(f"Kan geen verbinding maken met MQTT broker: {e}")
            # We gaan door, loop_start() probeert op de achtergrond opnieuw te verbinden

    def on_mqtt_connect(self, client, userdata, flags, rc, properties=None):
        """Callback bij succesvolle MQTT verbinding."""
        if rc == 0:
            mqtt_logger.info("Succesvol verbonden met MQTT Broker!")
            self.mqtt_connected = True
            
            # Abonneer op command topics voor alle schrijfbare sensoren
            topic_prefix = self.config.get("mqtt", {}).get("topic_prefix", "usr_n580_bridge")
            sensors = self.config.get("sensors", [])
            subscribed_count = 0
            for s in sensors:
                if s.get("writeable", False):
                    uid = s.get("unique_id")
                    entity_type = s.get("entity_type", "sensor")
                    cmd_topic = f"{topic_prefix}/{entity_type}/{uid}/set"
                    mqtt_logger.info(f"Abonneren op command topic: {cmd_topic}")
                    self.mqtt_client.subscribe(cmd_topic)
                    subscribed_count += 1
            if subscribed_count > 0:
                mqtt_logger.info(f"Geabonneerd op {subscribed_count} command topics.")
                    
            # Direct HA Discovery uitvoeren om entiteiten aan te maken/vernieuwen
            self.publish_ha_discovery()
            
            # Verzend alle gebufferde berichten
            self.flush_mqtt_buffer()
        else:
            mqtt_logger.error(f"MQTT Verbinding mislukt met code: {rc}")

    def on_mqtt_disconnect(self, client, userdata, disconnect_flags, rc, properties=None):
        """Callback bij MQTT ontkoppeling."""
        mqtt_logger.warning(f"MQTT verbinding verbroken (code: {rc}). Proberen opnieuw te verbinden...")
        self.mqtt_connected = False

    def on_mqtt_message(self, client, userdata, msg):
        """Callback bij ontvangen MQTT bericht (commando)."""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            mqtt_logger.info(f"MQTT bericht ontvangen op {topic}: {payload}")
            
            # Ontleed het topic: [topic_prefix]/[entity_type]/[unique_id]/set
            parts = topic.split('/')
            if len(parts) < 4 or parts[-1] != "set":
                return
                
            entity_type = parts[-3]
            uid = parts[-2]
            
            # Zoek de bijbehorende sensor in de configuratie
            sensors = self.config.get("sensors", [])
            sensor = None
            for s in sensors:
                if s.get("unique_id") == uid:
                    sensor = s
                    break
                    
            if not sensor:
                mqtt_logger.error(f"Geen sensor gevonden met unique_id: {uid}")
                return
                
            if not sensor.get("writeable", False):
                mqtt_logger.error(f"Sensor {uid} is niet gemarkeerd als schrijfbaar!")
                return
                
            # Schrijf de nieuwe waarde naar Modbus
            self.write_sensor(sensor, payload)
            
        except Exception as e:
            mqtt_logger.error(f"Fout bij verwerken MQTT bericht: {e}")

    def publish_ha_discovery(self):
        """Publiceer de Home Assistant MQTT Autodiscovery payloads."""
        if self.dry_run or not self.mqtt_connected:
            return

        mqtt_cfg = self.config.get("mqtt", {})
        disc_prefix = mqtt_cfg.get("discovery_prefix", "homeassistant")
        topic_prefix = mqtt_cfg.get("topic_prefix", "usr_n580_bridge")
        sensors = self.config.get("sensors", [])

        # 1. Verzenden van connectiviteitssensoren voor alle unieke slaves
        unique_slaves = sorted(list(set(s.get("slave", 1) for s in sensors)))
        mqtt_logger.info(f"Verzenden van Home Assistant Autodiscovery connectiviteitssensoren voor {len(unique_slaves)} apparaten...")
        
        for slave in unique_slaves:
            dev_name, dev_id = self.get_device_info_by_slave(slave)
            uid = f"{dev_id}_connectivity"
            name = f"{dev_name} Status"
            
            discovery_topic = f"{disc_prefix}/binary_sensor/{uid}/config"
            state_topic = f"{topic_prefix}/binary_sensor/{uid}/state"
            
            payload = {
                "name": name,
                "unique_id": uid,
                "state_topic": state_topic,
                "device_class": "connectivity",
                "payload_on": "ON",
                "payload_off": "OFF",
                "device": {
                    "identifiers": [f"modbus_usr_n580_device_{dev_id}"],
                    "name": dev_name,
                    "model": "Modbus RTU-over-TCP Device",
                    "manufacturer": "USR-N580 Gateway"
                }
            }
            
            try:
                mqtt_logger.debug(f"HA Discovery payload voor connectivity {uid}: {json.dumps(payload)}")
                self.mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True)
            except Exception as e:
                mqtt_logger.error(f"Fout bij publiceren connectivity discovery voor {uid}: {e}")

        # 2. Verzenden van reguliere sensoren
        mqtt_logger.info(f"Verzenden van {len(sensors)} Home Assistant Autodiscovery sensoren...")

        for s in sensors:
            uid = s.get("unique_id")
            name = s.get("name")
            unit = s.get("unit_of_measurement", "")
            dev_class = s.get("device_class", "")
            state_class = s.get("state_class", "")
            entity_type = s.get("entity_type", "sensor")

            if not uid or not name:
                continue

            discovery_topic = f"{disc_prefix}/{entity_type}/{uid}/config"
            state_topic = f"{topic_prefix}/{entity_type}/{uid}/state"
            cmd_topic = f"{topic_prefix}/{entity_type}/{uid}/set"

            # Bepaal apparaatnaam en identifiers via de helper
            slave = s.get("slave", 1)
            dev_name, dev_id = self.get_device_info_by_slave(slave)

            # Autodiscovery payload conform Home Assistant standaarden (zonder beschikbaarheidsbinding
            # zodat sensoren hun laatste waarde behouden als de verbinding wegvalt)
            payload = {
                "name": name,
                "unique_id": uid,
                "state_topic": state_topic,
                "device": {
                    "identifiers": [f"modbus_usr_n580_device_{dev_id}"],
                    "name": dev_name,
                    "model": "Modbus RTU-over-TCP Device",
                    "manufacturer": "USR-N580 Gateway"
                }
            }

            if entity_type == "switch":
                payload["value_template"] = "{{ 'ON' if value_json.value == 1 else 'OFF' }}"
                payload["command_topic"] = cmd_topic
                payload["payload_on"] = "ON"
                payload["payload_off"] = "OFF"
            elif entity_type == "select":
                payload["value_template"] = "{{ value_json.value }}"
                payload["command_topic"] = cmd_topic
                payload["options"] = s.get("options", [])
            elif entity_type == "number":
                payload["value_template"] = "{{ value_json.value }}"
                payload["command_topic"] = cmd_topic
                # Bepaal limieten op basis van sensornaam/type
                if "temp" in uid or "temperatuur" in name.lower():
                    payload["min"] = s.get("min", 15.0)
                    payload["max"] = s.get("max") or s.get("max_value") or 45.0
                    payload["step"] = s.get("step", 1.0)
                else:
                    payload["min"] = s.get("min", 0.0)
                    payload["max"] = s.get("max") or s.get("max_value") or 100.0
                    payload["step"] = s.get("step", 1.0)
            else: # "sensor"
                payload["value_template"] = "{{ value_json.value }}"
                if unit:
                    payload["unit_of_measurement"] = unit
                if dev_class:
                    payload["device_class"] = dev_class
                if state_class:
                    payload["state_class"] = state_class

            try:
                mqtt_logger.debug(f"HA Discovery payload voor {uid} ({entity_type}): {json.dumps(payload)}")
                self.mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True)
            except Exception as e:
                mqtt_logger.error(f"Fout bij publiceren discovery voor {uid}: {e}")

        mqtt_logger.info("Home Assistant Autodiscovery verzenden voltooid!")

    def publish_sensor_value(self, unique_id, value):
        """Verzend een uitgelezen registerwaarde naar de MQTT broker."""
        if self.dry_run:
            logger.info(f"[DRY-RUN] Publiceren {unique_id} -> {value}")
            return

        # Zoek het entiteitstype voor het juiste topic
        entity_type = "sensor"
        options = []
        for s in self.config.get("sensors", []):
            if s.get("unique_id") == unique_id:
                entity_type = s.get("entity_type", "sensor")
                options = s.get("options", [])
                break

        topic_prefix = self.config.get("mqtt", {}).get("topic_prefix", "usr_n580_bridge")
        state_topic = f"{topic_prefix}/{entity_type}/{unique_id}/state"
        
        # Mapping index to string for select type
        pub_value = value
        if entity_type == "select" and options:
            try:
                idx = int(round(float(value)))
                if 0 <= idx < len(options):
                    pub_value = options[idx]
            except (ValueError, TypeError):
                pass

        # Update de status cache voor de REST API en Dashboard
        self.sensor_values[unique_id] = {
            "value": pub_value,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Bufferen indien MQTT offline is
        if not self.mqtt_connected:
            with self.buffer_lock:
                self.mqtt_buffer[state_topic] = (json.dumps({"value": pub_value}), False)
            mqtt_logger.debug(f"MQTT offline. Waarde voor {unique_id} ({pub_value}) gebufferd.")
            return

        # Controleer of de waarde veranderd is ten opzichte van de vorige keer
        if getattr(self, "last_published_values", None) is None:
            self.last_published_values = {}

        if self.last_published_values.get(unique_id) == pub_value:
            mqtt_logger.debug(f"Waarde voor {unique_id} is ongewijzigd ({pub_value}). Publiceren overgeslagen.")
            return

        payload = json.dumps({"value": pub_value})

        try:
            mqtt_logger.info(f"Publiceer: {state_topic} -> {payload}")
            self.mqtt_client.publish(state_topic, payload)
            self.last_published_values[unique_id] = pub_value
        except Exception as e:
            mqtt_logger.error(f"Fout bij publiceren status voor {unique_id}: {e}")

    def publish_slave_connectivity(self, slave, status):
        """Publiceer de connectiviteitsstatus (ON/OFF) voor een specifieke slave."""
        if self.dry_run:
            logger.info(f"[DRY-RUN] Publiceren connectiviteit Slave {slave} -> {status}")
            return

        if getattr(self, "slave_states", None) is None:
            self.slave_states = {}
        
        # Wijzig status cache voor API
        self.slave_states[slave] = status

        _, dev_id = self.get_device_info_by_slave(slave)
        topic_prefix = self.config.get("mqtt", {}).get("topic_prefix", "usr_n580_bridge")
        state_topic = f"{topic_prefix}/binary_sensor/{dev_id}_connectivity/state"

        # Bufferen indien MQTT offline is
        if not self.mqtt_connected:
            with self.buffer_lock:
                self.mqtt_buffer[state_topic] = (status, True)
            mqtt_logger.debug(f"MQTT offline. Status voor slave {slave} ({status}) gebufferd.")
            return

        if not hasattr(self, "last_published_slave_states"):
            self.last_published_slave_states = {}

        if self.last_published_slave_states.get(slave) == status:
            return

        try:
            mqtt_logger.info(f"Slave {slave} is nu {status}. Publiceer naar {state_topic}")
            self.mqtt_client.publish(state_topic, status, retain=True)
            self.last_published_slave_states[slave] = status
        except Exception as e:
            mqtt_logger.error(f"Fout bij publiceren connectiviteit voor Slave {slave}: {e}")

    def write_sensor(self, sensor, payload):
        """Schrijf een waarde naar Modbus op basis van de MQTT payload."""
        uid = sensor.get("unique_id")
        slave = sensor.get("slave", 1)
        address = sensor.get("address", 0)
        scale = sensor.get("scale", 1.0)
        entity_type = sensor.get("entity_type", "sensor")
        
        modbus_logger.info(f"Schrijven naar {uid} (Slave {slave}, Adres {address}): Payload = {payload}")
        
        # 1. Converteer payload naar modbus integer waarde
        try:
            if entity_type == "switch":
                payload_clean = payload.strip().upper()
                if payload_clean in ("ON", "1", "TRUE"):
                    val = 1
                elif payload_clean in ("OFF", "0", "FALSE"):
                    val = 0
                else:
                    val = int(payload)
            elif entity_type == "select":
                options = sensor.get("options", [])
                if payload in options:
                    val = options.index(payload)
                else:
                    try:
                        val = int(round(float(payload)))
                    except ValueError:
                        modbus_logger.error(f"Ongeldige optie '{payload}' voor select {uid}. Beschikbare opties: {options}")
                        return
            else:
                # Converteer naar float en pas inverse scaling toe
                float_val = float(payload)
                val = int(round(float_val / scale))
                
            # Zorg dat de waarde in een 16-bits register past (handling signed/unsigned int16)
            if sensor.get("data_type", "int16") == "int16" and val < 0:
                val += 65536
                
            val = max(0, min(65535, val))
            
        except ValueError as e:
            modbus_logger.error(f"Kan payload '{payload}' niet converteren naar Modbus waarde voor {uid}: {e}")
            return
            
        # 2. Modbus verbinding controleren/tot stand brengen
        if not self.modbus_client or not self.modbus_client.connected:
            modbus_logger.warning("Modbus client niet verbonden. Poging tot herverbinden...")
            if not self.connect_modbus():
                return
                
        # 3. Schrijf naar het Modbus register
        try:
            if self.dry_run:
                modbus_logger.info(f"[DRY-RUN] Schrijven naar register {address} op slave {slave} met waarde {val}")
                if entity_type == "switch":
                    pub_val = 1 if val == 1 else 0
                elif entity_type == "select":
                    pub_val = val
                else:
                    pub_val = float(payload)
                self.publish_sensor_value(uid, pub_val)
                return
                
            # Alleen holding registers kunnen geschreven worden in Modbus
            reg_type = sensor.get("register_type", "holding")
            if reg_type != "holding":
                modbus_logger.error(f"Fout: Alleen holding registers zijn schrijfbaar (sensor {uid} heeft type {reg_type})")
                return
                
            res = self.modbus_client.write_register(address=address, value=val, device_id=slave)
            if res.isError():
                modbus_logger.error(f"Fout bij schrijven naar Modbus voor {uid}: {res}")
                if self.modbus_client:
                    self.modbus_client.close()
            else:
                modbus_logger.info(f"Succesvol geschreven naar Modbus voor {uid}: Raw {val}")
                # Direct de nieuwe status terug publiceren naar MQTT zodat de UI meteen geüpdatet wordt!
                time.sleep(0.2)
                actual_val = self.read_sensor(sensor)
                if actual_val is not None:
                    self.publish_sensor_value(uid, actual_val)
                    
        except Exception as e:
            modbus_logger.error(f"Exception bij schrijven naar Modbus voor {uid}: {e}")
            if self.modbus_client:
                try:
                    self.modbus_client.close()
                except Exception:
                    pass


    # --------------------------------------------------------------------------
    # Modbus Functionaliteiten
    # --------------------------------------------------------------------------
    def connect_modbus(self):
        """Maak verbinding met de USR-N580 Modbus Gateway."""
        modbus_cfg = self.config.get("modbus", {})
        host = modbus_cfg.get("host", "192.168.50.96")
        port = modbus_cfg.get("port", 41)
        timeout = modbus_cfg.get("timeout", 3)

        modbus_logger.info(f"Verbinden met USR-N580 Gateway op {host}:{port} (Modbus RTU-over-TCP)...")
        
        # Belangrijk: USR-N580 gebruikt RTU framing over TCP met retries uitgeschakeld voor direct falen
        self.modbus_client = ModbusTcpClient(
            host, 
            port=port, 
            framer=FramerType.RTU, 
            timeout=timeout,
            retries=0
        )

        if self.modbus_client.connect():
            modbus_logger.info("Modbus verbinding succesvol tot stand gebracht!")
            return True
        else:
            modbus_logger.error("Modbus verbinding mislukt! Controleer het IP, de poort en netwerkkabels.")
            return False

    def read_sensor(self, sensor):
        """Lees een specifieke sensor uit via Modbus."""
        if not self.modbus_client or not self.modbus_client.connected:
            modbus_logger.warning("Modbus client niet verbonden. Poging tot herverbinden...")
            if not self.connect_modbus():
                return None

        slave = sensor.get("slave", 1)
        reg_type = sensor.get("register_type", "holding")
        address = sensor.get("address", 0)
        scale = sensor.get("scale", 1.0)
        precision = sensor.get("precision", 0)
        uid = sensor.get("unique_id", "unknown")

        modbus_logger.debug(f"Lezen van {uid} (Slave: {slave}, Type: {reg_type}, Adres: {address})")

        try:
            count = 2 if sensor.get("data_type") in ("int32", "uint32") else 1
            if reg_type == "holding":
                res = self.modbus_client.read_holding_registers(address=address, count=count, device_id=slave)
            elif reg_type == "input":
                res = self.modbus_client.read_input_registers(address=address, count=count, device_id=slave)
            else:
                modbus_logger.error(f"Onbekend register type: {reg_type} voor {uid}")
                return None

            if res.isError():
                modbus_logger.error(f"Fout bij uitlezen {uid} (Slave {slave}, Adres {address}): {res}")
                if self.modbus_client:
                    self.modbus_client.close()
                return None

            # Interpreteer de waarde (meestal een signed int16 of 32-bits int/uint)
            if count == 2:
                # Modbus 32-bits combineert twee 16-bits registers (High Word eerst / Big-Endian)
                raw_value = (res.registers[0] << 16) | res.registers[1]
                if sensor.get("data_type") == "int32" and raw_value >= 2147483648:
                    raw_value -= 4294967296
                debug_raw = f"{res.registers[0]},{res.registers[1]} ({raw_value})"
            else:
                raw_value = res.registers[0]
                # Verwerk signed integers handmatig indien int16
                if sensor.get("data_type", "int16") == "int16" and raw_value >= 32768:
                    raw_value -= 65536
                debug_raw = str(res.registers[0])

            # Toepassen van schaling en precisie
            calculated_value = round(raw_value * scale, precision) if precision > 0 else int(round(raw_value * scale))
            
            modbus_logger.debug(f"Gelezen {uid}: Raw={debug_raw} -> Berekend={calculated_value}")
            return calculated_value

        except Exception as e:
            modbus_logger.error(f"Exception bij uitlezen {uid}: {e}")
            if self.modbus_client:
                try:
                    self.modbus_client.close()
                except Exception:
                    pass
            return None

    # --------------------------------------------------------------------------
    # Hoofd Polling Loop
    # --------------------------------------------------------------------------
    def poll_all_sensors(self):
        """Lees alle geconfigureerde sensoren en publiceer hun waarden."""
        sensors = self.config.get("sensors", [])
        logger.info(f"Starten pollingronde voor {len(sensors)} sensoren...")
        
        self.stats["total_polls"] += 1
        
        failed_slaves = set()
        success_count = 0
        slaves_marked_online = set()
        
        for s in sensors:
            slave = s.get("slave", 1)
            uid = s.get("unique_id")
            
            # Sla over als deze slave al gemarkeerd is als mislukt in deze pollingronde
            if slave in failed_slaves:
                modbus_logger.debug(f"Sla {uid} over omdat Slave {slave} in deze ronde al gefaald is.")
                self.stats["failed_reads"] += 1
                continue
                
            val = self.read_sensor(s)
            
            if val is not None:
                self.publish_sensor_value(uid, val)
                success_count += 1
                self.stats["successful_reads"] += 1
                
                # Markeer als online als we dat nog niet hebben gedaan in deze ronde
                if slave not in slaves_marked_online:
                    self.publish_slave_connectivity(slave, "ON")
                    slaves_marked_online.add(slave)
            else:
                # Markeer als mislukt zodat we overige registers voor deze slave overslaan in deze ronde
                failed_slaves.add(slave)
                self.publish_slave_connectivity(slave, "OFF")
                self.stats["failed_reads"] += 1
                modbus_logger.warning(f"Slave {slave} reageert niet. Resterende sensors voor deze slave in deze pollingronde worden overgeslagen.")
                
            # Wacht heel even tussen verzoeken om bus-collisies op de RS485-lijn te minimaliseren
            time.sleep(0.1)
            
        logger.info(f"Pollingronde voltooid. {success_count}/{len(sensors)} succesvol uitgelezen.")

    def run_once(self):
        """Voer één enkele polling uit en sluit af (handig voor testen/CLI)."""
        logger.info("--- START TEST-ONCE RUN ---")
        
        # Modbus verbinden
        self.connect_modbus()
        
        # MQTT verbinden en kort wachten voor verbinding
        self.setup_mqtt()
        if not self.dry_run:
            time.sleep(2)
            
        # Poll en publiceer
        self.poll_all_sensors()
        
        # Netjes afsluiten
        if self.modbus_client:
            self.modbus_client.close()
            logger.info("Modbus verbinding gesloten.")
            
        if self.mqtt_client and not self.dry_run:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            logger.info("MQTT verbinding gesloten.")
            
        logger.info("--- EINDE TEST-ONCE RUN ---")

    def run_daemon(self):
        """Start de bridge in daemon-mode (continue loop)."""
        logger.info("=== Starten Modbus-MQTT Bridge Daemon ===")
        
        # 1. Start Modbus verbinding
        self.connect_modbus()
        
        # 2. Start MQTT verbinding
        self.setup_mqtt()
        
        # 3. Polling loop starten
        poll_interval = self.config.get("modbus", {}).get("poll_interval", 15)
        logger.info(f"Daemon actief. Polling interval: {poll_interval} seconden.")
        
        while self.running:
            start_time = time.time()
            
            try:
                self.poll_all_sensors()
            except Exception as e:
                logger.error(f"Onverwachte fout in polling loop: {e}")
                
            # Bereken resterende slaaptijd tot de volgende interval
            elapsed = time.time() - start_time
            sleep_time = max(0.1, poll_interval - elapsed)
            
            # Slaap in kleine stapjes zodat we snel reageren op afsluit-signalen (SIGINT/SIGTERM)
            for _ in range(int(sleep_time * 10)):
                if not self.running:
                    break
                time.sleep(0.1)
                
        # Netjes afsluiten
        logger.info("Daemon stopt. Verbindingen worden gesloten...")
        if self.modbus_client:
            self.modbus_client.close()
        if self.mqtt_client and not self.dry_run:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        logger.info("Bridge is volledig afgesloten. Tot ziens!")

    def flush_mqtt_buffer(self):
        """Verzend alle gebufferde berichten naar de MQTT broker."""
        with self.buffer_lock:
            if not self.mqtt_buffer:
                return
            mqtt_logger.info(f"MQTT verbinding hersteld. {len(self.mqtt_buffer)} gebufferde berichten verzenden...")
            buffer_copy = dict(self.mqtt_buffer)
            self.mqtt_buffer.clear()

        for topic, (payload, retain) in buffer_copy.items():
            try:
                mqtt_logger.debug(f"Verzenden gebufferd bericht: {topic} -> {payload} (retain={retain})")
                self.mqtt_client.publish(topic, payload, retain=retain)
            except Exception as e:
                mqtt_logger.error(f"Fout bij verzenden gebufferd bericht op {topic}: {e}")
                with self.buffer_lock:
                    self.mqtt_buffer[topic] = (payload, retain)

    def start_http_server(self):
        """Start de Ingress HTTP status server in een aparte thread."""
        try:
            port = 8099
            self.http_server = StatusHTTPServer(('0.0.0.0', port), StatusHTTPRequestHandler, self)
            self.http_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
            self.http_thread.start()
            logger.info(f"Ingress HTTP status server gestart op poort {port}")
        except Exception as e:
            logger.error(f"Kan Ingress HTTP server niet starten: {e}")

    def get_status_json(self):
        """Genereer een JSON status representatie van de daemon."""
        with self.buffer_lock:
            buffer_size = len(self.mqtt_buffer)
        
        uptime = int(time.time() - self.start_time)
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
        minutes = (uptime % 3600) // 60
        seconds = uptime % 60
        
        uptime_parts = []
        if days > 0: uptime_parts.append(f"{days}d")
        if hours > 0: uptime_parts.append(f"{hours}h")
        if minutes > 0: uptime_parts.append(f"{minutes}m")
        uptime_parts.append(f"{seconds}s")
        uptime_str = " ".join(uptime_parts)

        sensor_list = []
        for s in self.config.get("sensors", []):
            uid = s.get("unique_id")
            name = s.get("name")
            slave = s.get("slave", 1)
            val_data = self.sensor_values.get(uid, {"value": "Nog geen meting", "timestamp": "-"})
            sensor_list.append({
                "unique_id": uid,
                "name": name,
                "slave": slave,
                "value": val_data["value"],
                "timestamp": val_data["timestamp"],
                "unit": s.get("unit_of_measurement", "")
            })

        return {
            "status": "running" if self.running else "stopped",
            "uptime": uptime_str,
            "mqtt": {
                "connected": self.mqtt_connected,
                "broker": self.config.get("mqtt", {}).get("broker", "localhost"),
                "buffer_size": buffer_size
            },
            "modbus": {
                "connected": self.modbus_client.connected if (self.modbus_client and hasattr(self.modbus_client, "connected")) else False,
                "host": self.config.get("modbus", {}).get("host", "localhost")
            },
            "stats": self.stats,
            "slave_states": self.slave_states,
            "sensors": sensor_list
        }

    def get_status_html(self, ingress_path=""):
        """Genereer de HTML-pagina voor het status dashboard."""
        return HTML_TEMPLATE.replace("{{INGRESS_PATH}}", ingress_path)

    def reload_configuration(self, new_yaml_content):
        """Valideer de ingevoerde YAML, schrijf deze naar config.yaml en herlaad verbindingen."""
        try:
            parsed_config = yaml.safe_load(new_yaml_content)
            if not isinstance(parsed_config, dict):
                raise ValueError("Configuratie moet een YAML dictionary/object zijn.")
        except Exception as e:
            raise ValueError(f"Fout bij parsen van YAML: {e}")

        # Schrijf naar bestand
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write(new_yaml_content)

        logger.info(f"Configuratiebestand '{self.config_path}' bijgewerkt via Web UI. Hot-reloading...")

        # Bewaar oude netwerkconfiguratie om te vergelijken
        old_mqtt = self.config.get("mqtt", {})
        old_modbus = self.config.get("modbus", {})

        # Herlaad configuratie in geheugen
        self.load_config()

        # Vergelijk MQTT instellingen en herstart verbinding indien nodig
        new_mqtt = self.config.get("mqtt", {})
        mqtt_changed = (
            old_mqtt.get("broker") != new_mqtt.get("broker") or
            old_mqtt.get("port") != new_mqtt.get("port") or
            old_mqtt.get("username") != new_mqtt.get("username") or
            old_mqtt.get("password") != new_mqtt.get("password") or
            old_mqtt.get("topic_prefix") != new_mqtt.get("topic_prefix")
        )

        if mqtt_changed and not self.dry_run:
            mqtt_logger.info("MQTT configuratie gewijzigd via Web UI. Opnieuw verbinden...")
            if self.mqtt_client:
                try:
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.disconnect()
                except Exception as e:
                    mqtt_logger.debug(f"Fout bij afsluiten MQTT verbinding: {e}")
            self.setup_mqtt()

        # Vergelijk Modbus instellingen en herstart verbinding indien nodig
        new_modbus = self.config.get("modbus", {})
        modbus_changed = (
            old_modbus.get("host") != new_modbus.get("host") or
            old_modbus.get("port") != new_modbus.get("port") or
            old_modbus.get("timeout") != new_modbus.get("timeout")
        )

        if modbus_changed and not self.dry_run:
            modbus_logger.info("Modbus configuratie gewijzigd via Web UI. Opnieuw verbinden...")
            if self.modbus_client:
                try:
                    self.modbus_client.close()
                except Exception as e:
                    modbus_logger.debug(f"Fout bij sluiten Modbus client: {e}")
            self.connect_modbus()

        # Re-publiceer Home Assistant Discovery met eventueel nieuwe sensoren
        if not self.dry_run and self.mqtt_connected:
            mqtt_logger.info("Re-publiceren Home Assistant Discovery voor nieuwe/gewijzigde sensoren...")
            self.publish_ha_discovery()

        logger.info("Configuratie live herladen voltooid!")

    def terminate(self):
        """Zet de vlag om de daemon te stoppen."""
        self.running = False
        if hasattr(self, "http_server"):
            logger.info("Ingress HTTP server stoppen...")
            try:
                self.http_server.shutdown()
                self.http_server.server_close()
            except Exception as e:
                logger.error(f"Fout bij stoppen HTTP server: {e}")


# ==============================================================================
# Ingress HTTP Server & Handler
# ==============================================================================
class StatusHTTPServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bridge):
        super().__init__(server_address, RequestHandlerClass)
        self.bridge = bridge


class StatusHTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Override om de stdout logs schoon te houden
        logger.debug(f"HTTP Server: {format % args}")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Ingress-Path")
        self.end_headers()

    def do_GET(self):
        path = self.path
        if path.endswith("/api/status"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            status_data = self.server.bridge.get_status_json()
            self.wfile.write(json.dumps(status_data).encode('utf-8'))
        elif path.endswith("/api/config"):
            self.send_response(200)
            self.send_header("Content-Type", "text/yaml; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                with open(self.server.bridge.config_path, 'r', encoding='utf-8') as f:
                    config_content = f.read()
            except Exception as e:
                config_content = f"# Fout bij lezen van bestand: {e}"
            self.wfile.write(config_content.encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            
            ingress_path = self.headers.get("X-Ingress-Path", "")
            if ingress_path.endswith("/"):
                ingress_path = ingress_path[:-1]
                
            html_content = self.server.bridge.get_status_html(ingress_path=ingress_path)
            self.wfile.write(html_content.encode('utf-8'))

    def do_POST(self):
        path = self.path
        if path.endswith("/api/config"):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                self.server.bridge.reload_configuration(post_data)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "message": "Configuratie succesvol bijgewerkt en herladen!"}).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()


# ==============================================================================
# Status Dashboard HTML Sjabloon (Premium Dark Glassmorphism)
# ==============================================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="nl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Modbus-MQTT Bridge Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0f19;
            --card-bg: rgba(255, 255, 255, 0.03);
            --card-border: rgba(255, 255, 255, 0.07);
            --text-color: #f3f4f6;
            --text-muted: #9ca3af;
            --primary: #3b82f6;
            --primary-glow: rgba(59, 130, 246, 0.15);
            --success: #10b981;
            --success-glow: rgba(16, 185, 129, 0.15);
            --danger: #ef4444;
            --danger-glow: rgba(239, 68, 68, 0.15);
            --warning: #f59e0b;
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            font-family: 'Outfit', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
            padding: 2rem;
            min-height: 100vh;
            background-image: 
                radial-gradient(at 0% 0%, rgba(59, 130, 246, 0.05) 0px, transparent 50%),
                radial-gradient(at 100% 0%, rgba(16, 185, 129, 0.05) 0px, transparent 50%);
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid var(--card-border);
        }
        
        h1 {
            font-size: 2rem;
            font-weight: 800;
            background: linear-gradient(135deg, #fff 0%, #a5b4fc 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .badge {
            padding: 0.35rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
        }
        
        .badge::before {
            content: '';
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }
        
        .badge-success {
            background-color: var(--success-glow);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }
        .badge-success::before { background-color: var(--success); }
        
        .badge-danger {
            background-color: var(--danger-glow);
            color: var(--danger);
            border: 1px solid rgba(239, 68, 68, 0.2);
        }
        .badge-danger::before { background-color: var(--danger); }
        
        /* Navigation Tabs */
        .nav-tabs {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--card-border);
            padding-bottom: 1px;
        }
        
        .tab-btn {
            background: none;
            border: none;
            color: var(--text-muted);
            padding: 0.75rem 1.5rem;
            font-size: 1rem;
            font-weight: 600;
            font-family: inherit;
            cursor: pointer;
            border-bottom: 2px solid transparent;
            transition: color 0.2s, border-color 0.2s;
            outline: none;
        }
        
        .tab-btn:hover {
            color: var(--text-color);
        }
        
        .tab-btn.active {
            color: var(--primary);
            border-bottom-color: var(--primary);
        }
        
        .tab-content {
            display: none;
        }
        
        .tab-content.active {
            display: block;
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }
        
        .card {
            background: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.2);
            transition: transform 0.2s, border-color 0.2s;
        }
        
        .card:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.12);
        }
        
        .card-title {
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 0.75rem;
            font-weight: 600;
        }
        
        .card-value {
            font-size: 1.8rem;
            font-weight: 800;
            display: flex;
            align-items: baseline;
            gap: 0.5rem;
        }
        
        .card-value span {
            font-size: 1rem;
            font-weight: 400;
            color: var(--text-muted);
        }
        
        .section-title {
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 1.25rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.2);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
        }
        
        th, td {
            padding: 1rem 1.25rem;
            text-align: left;
        }
        
        th {
            background-color: rgba(255, 255, 255, 0.02);
            font-weight: 600;
            color: var(--text-muted);
            border-bottom: 1px solid var(--card-border);
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        tr:not(:last-child) td {
            border-bottom: 1px solid var(--card-border);
        }
        
        tr:hover td {
            background-color: rgba(255, 255, 255, 0.01);
        }
        
        .sensor-val {
            font-family: monospace;
            font-size: 1.1rem;
            font-weight: 600;
            color: #fff;
        }
        
        .text-center {
            text-align: center;
            padding: 2rem;
            color: var(--text-muted);
        }
        
        /* Editor Elements */
        .editor-container {
            display: flex;
            flex-direction: column;
            gap: 1.25rem;
        }
        
        .yaml-textarea {
            width: 100%;
            height: 550px;
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid var(--card-border);
            border-radius: 12px;
            color: #e5e7eb;
            font-family: 'Courier New', Courier, monospace;
            font-size: 0.95rem;
            line-height: 1.5;
            padding: 1.25rem;
            resize: vertical;
            outline: none;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        
        .yaml-textarea:focus {
            border-color: var(--primary);
            box-shadow: 0 0 10px var(--primary-glow);
        }
        
        .editor-actions {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 0.5rem;
        }
        
        .btn {
            padding: 0.75rem 1.75rem;
            border-radius: 10px;
            font-size: 0.95rem;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s, transform 0.1s;
            border: none;
            outline: none;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .btn-primary {
            background-color: var(--primary);
            color: #fff;
        }
        
        .btn-primary:hover {
            background-color: #2563eb;
        }
        
        .btn-primary:active {
            transform: scale(0.98);
        }
        
        .alert-box {
            padding: 1rem 1.25rem;
            border-radius: 12px;
            font-weight: 500;
            display: flex;
            align-items: flex-start;
            gap: 0.75rem;
            border: 1px solid transparent;
            margin-bottom: 1.5rem;
        }
        
        .alert-box-success {
            background-color: var(--success-glow);
            color: var(--success);
            border-color: rgba(16, 185, 129, 0.2);
        }
        
        .alert-box-danger {
            background-color: var(--danger-glow);
            color: var(--danger);
            border-color: rgba(239, 68, 68, 0.2);
            white-space: pre-wrap;
            font-family: monospace;
            font-size: 0.9rem;
        }
        
        .footer {
            margin-top: 3rem;
            text-align: center;
            font-size: 0.85rem;
            color: var(--text-muted);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>Modbus-MQTT Bridge</h1>
                <p style="color: var(--text-muted); font-size: 0.9rem;">Status & Monitoring Console</p>
            </div>
            <div id="bridge-status-badge">
                <span class="badge badge-danger">Laden...</span>
            </div>
        </header>

        <div class="nav-tabs">
            <button id="btn-tab-dashboard" class="tab-btn active" onclick="switchTab('dashboard')">📊 Status Dashboard</button>
            <button id="btn-tab-editor" class="tab-btn" onclick="switchTab('editor')">📝 Configuratie Editor</button>
        </div>
        
        <!-- Tab 1: Dashboard -->
        <div id="tab-dashboard" class="tab-content active">
            <div class="grid">
                <div class="card">
                    <div class="card-title">Uptime</div>
                    <div class="card-value" id="uptime-val">-</div>
                </div>
                <div class="card">
                    <div class="card-title">Modbus Gateway</div>
                    <div class="card-value" id="modbus-val" style="font-size: 1.4rem;">-</div>
                    <div id="modbus-status-badge" style="margin-top: 0.5rem;"></div>
                </div>
                <div class="card">
                    <div class="card-title">MQTT Connection</div>
                    <div class="card-value" id="mqtt-val" style="font-size: 1.4rem;">-</div>
                    <div id="mqtt-status-badge" style="margin-top: 0.5rem;"></div>
                </div>
                <div class="card">
                    <div class="card-title">Statistieken (Polls)</div>
                    <div class="card-value" id="stats-val" style="font-size: 1.3rem;">
                        Success: <span id="stats-success" style="color: var(--success);">0</span> 
                        | Fouten: <span id="stats-failed" style="color: var(--danger);">0</span>
                    </div>
                    <div style="margin-top: 0.5rem; font-size: 0.85rem; color: var(--text-muted);">
                        Totaal rondes: <span id="stats-total">0</span>
                    </div>
                </div>
            </div>
            
            <div id="buffer-alert-container"></div>
            
            <div class="section-title">📊 Live Sensor Waarden</div>
            <table>
                <thead>
                    <tr>
                        <th>Naam / ID</th>
                        <th>Slave</th>
                        <th>Huidige Waarde</th>
                        <th>Laatst Geüpdatet</th>
                    </tr>
                </thead>
                <tbody id="sensor-table-body">
                    <tr>
                        <td colspan="4" class="text-center">Gegevens laden...</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <!-- Tab 2: Editor -->
        <div id="tab-editor" class="tab-content">
            <div class="editor-container">
                <div class="section-title">📝 Wijzig config.yaml</div>
                <div id="editor-alert-container"></div>
                <textarea id="config-textarea" class="yaml-textarea" spellcheck="false" placeholder="# Laden van configuratie..."></textarea>
                <div class="editor-actions">
                    <span id="editor-status-text" style="color: var(--text-muted); font-size: 0.9rem;"></span>
                    <button class="btn btn-primary" onclick="saveConfiguration()">💾 Opslaan & Toepassen</button>
                </div>
            </div>
        </div>
        
        <div class="footer">
            Modbus-MQTT Bridge Add-on v1.0.14 • Ontwikkeld voor Raspberry Pi & Home Assistant
        </div>
    </div>

    <script>
        const ingressPath = "{{INGRESS_PATH}}";
        const apiURL = ingressPath + "/api/status";
        const configURL = ingressPath + "/api/config";
        
        let activeTab = 'dashboard';
        
        function switchTab(tabId) {
            activeTab = tabId;
            
            // Buttons class
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.getElementById(`btn-tab-${tabId}`).classList.add('active');
            
            // Contents class
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            document.getElementById(`tab-${tabId}`).classList.add('active');
            
            if (tabId === 'editor') {
                loadConfiguration();
            }
        }
        
        function loadConfiguration() {
            const statusText = document.getElementById('editor-status-text');
            statusText.innerText = "Bezig met inladen van bestand...";
            
            fetch(configURL)
                .then(response => {
                    if (!response.ok) throw new Error("Fout status code: " + response.status);
                    return response.text();
                })
                .then(yaml => {
                    document.getElementById('config-textarea').value = yaml;
                    statusText.innerText = "Configuratie succesvol geladen.";
                    document.getElementById('editor-alert-container').innerHTML = '';
                })
                .catch(err => {
                    console.error("Fout bij inladen config:", err);
                    statusText.innerText = "Laden mislukt.";
                    document.getElementById('editor-alert-container').innerHTML = `
                        <div class="alert-box alert-box-danger">
                            <strong>❌ Kan config.yaml niet laden:</strong>\\n${err.message || err}
                        </div>
                    `;
                });
        }
        
        function saveConfiguration() {
            const statusText = document.getElementById('editor-status-text');
            const alertContainer = document.getElementById('editor-alert-container');
            const yamlContent = document.getElementById('config-textarea').value;
            
            statusText.innerText = "Opslaan en valideren...";
            alertContainer.innerHTML = '';
            
            fetch(configURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain'
                },
                body: yamlContent
            })
            .then(response => response.json().then(data => ({ status: response.status, data })))
            .then(({ status, data }) => {
                if (status === 200 && data.success) {
                    statusText.innerText = "Configuratie succesvol herladen!";
                    alertContainer.innerHTML = `
                        <div class="alert-box alert-box-success">
                            <strong>✅ Succes:</strong> ${data.message}
                        </div>
                    `;
                } else {
                    statusText.innerText = "Toepassen mislukt.";
                    alertContainer.innerHTML = `
                        <div class="alert-box alert-box-danger">
                            <strong>❌ Validatie- / Toepassingsfout:</strong>\\n${data.error || 'Onbekende fout'}
                        </div>
                    `;
                }
            })
            .catch(err => {
                console.error("Netwerkfout bij opslaan:", err);
                statusText.innerText = "Verbindingsfout.";
                alertContainer.innerHTML = `
                    <div class="alert-box alert-box-danger">
                        <strong>❌ Netwerkfout bij opslaan:</strong>\\n${err.message || err}
                    </div>
                `;
            });
        }
        
        function updateDashboard() {
            if (activeTab !== 'dashboard') return;
            
            fetch(apiURL)
                .then(response => response.json())
                .then(data => {
                    // Update status badge
                    const statusBadge = document.getElementById('bridge-status-badge');
                    if (data.status === 'running') {
                        statusBadge.innerHTML = '<span class="badge badge-success">Actief</span>';
                    } else {
                        statusBadge.innerHTML = '<span class="badge badge-danger">Gestopt</span>';
                    }
                    
                    // Update cards
                    document.getElementById('uptime-val').innerText = data.uptime;
                    
                    document.getElementById('modbus-val').innerText = data.modbus.host;
                    const modbusBadge = document.getElementById('modbus-status-badge');
                    if (data.modbus.connected) {
                        modbusBadge.innerHTML = '<span class="badge badge-success">Verbonden</span>';
                    } else {
                        modbusBadge.innerHTML = '<span class="badge badge-danger">Verbinding Verbroken</span>';
                    }
                    
                    document.getElementById('mqtt-val').innerText = data.mqtt.broker;
                    const mqttBadge = document.getElementById('mqtt-status-badge');
                    if (data.mqtt.connected) {
                        mqttBadge.innerHTML = '<span class="badge badge-success">Verbonden</span>';
                    } else {
                        mqttBadge.innerHTML = '<span class="badge badge-danger">Offline</span>';
                    }
                    
                    // Stats
                    document.getElementById('stats-success').innerText = data.stats.successful_reads;
                    document.getElementById('stats-failed').innerText = data.stats.failed_reads;
                    document.getElementById('stats-total').innerText = data.stats.total_polls;
                    
                    // Buffer Alert
                    const bufferContainer = document.getElementById('buffer-alert-container');
                    if (data.mqtt.buffer_size > 0) {
                        bufferContainer.innerHTML = `
                            <div class="card" style="border-color: var(--warning); background-color: rgba(245, 158, 11, 0.05); margin-bottom: 2rem; padding: 1rem;">
                                <span style="color: var(--warning); font-weight: 600;">⚠️ MQTT offline waarschuwing:</span> 
                                Er staan momenteel <strong>${data.mqtt.buffer_size}</strong> berichten in de wachtrij. 
                                Deze worden verzonden zodra de verbinding hersteld is.
                            </div>
                        `;
                    } else {
                        bufferContainer.innerHTML = '';
                    }
                    
                    // Sensor Table
                    const tbody = document.getElementById('sensor-table-body');
                    if (data.sensors.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="4" class="text-center">Geen sensoren geconfigureerd.</td></tr>';
                    } else {
                        let html = '';
                        data.sensors.forEach(s => {
                            html += `
                                <tr>
                                    <td>
                                        <div style="font-weight: 600;">${s.name}</div>
                                        <div style="font-size: 0.8rem; color: var(--text-muted); font-family: monospace;">${s.unique_id}</div>
                                    </td>
                                    <td><span class="badge" style="background: rgba(255,255,255,0.05); border: 1px solid var(--card-border); color: #fff;">Slave ${s.slave}</span></td>
                                    <td><span class="sensor-val">${s.value} ${s.unit}</span></td>
                                    <td style="color: var(--text-muted); font-size: 0.9rem;">${s.timestamp}</td>
                                </tr>
                            `;
                        });
                        tbody.innerHTML = html;
                    }
                })
                .catch(err => {
                    console.error("Fout bij ophalen status:", err);
                    document.getElementById('bridge-status-badge').innerHTML = '<span class="badge badge-danger">Fout bij laden</span>';
                });
        }
        
        // Initial call
        updateDashboard();
        // Periodieke verversing elke 5 seconden
        setInterval(updateDashboard, 5000);
    </script>
</body>
</html>
"""



# ==============================================================================
# Signaal Afhandeling & Entry Point
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Modbus naar MQTT Bridge voor USR-N580 Gateways.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-c", "--config", 
        default="config.yaml",
        help="Pad naar het YAML configuratiebestand (Standaard: config.yaml)"
    )
    parser.add_argument(
        "-d", "--debug", 
        action="store_true",
        help="Schakel uitgebreide debug logging in"
    )
    parser.add_argument(
        "-t", "--test-once", 
        action="store_true",
        help="Voer één enkele pollingronde uit en sluit af (handig voor testen)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Simuleer de werking zonder daadwerkelijk verbinding te maken met MQTT of registers aan te passen"
    )
    
    args = parser.parse_args()
    
    # Maak instantie van de bridge
    bridge = ModbusMqttBridge(
        config_path=args.config,
        debug=args.debug,
        dry_run=args.dry_run
    )
    
    # Registreer signalen voor netjes afsluiten (SIGINT, SIGTERM)
    def sig_handler(signum, frame):
        logger.warning(f"Afsluitsignaal ontvangen ({signum}). Stoppen...")
        bridge.terminate()
        
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    
    # Uitvoeren in de gekozen modus
    if args.test_once:
        bridge.run_once()
    else:
        bridge.run_daemon()

if __name__ == "__main__":
    main()
