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
try:
    from http.server import ThreadingHTTPServer
except ImportError:
    ThreadingHTTPServer = HTTPServer
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType
import paho.mqtt.client as mqtt
from urllib.parse import urlparse, parse_qs

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
        self.modbus_lock = threading.RLock()
        self.active_tasks = {}
        self.tasks_lock = threading.Lock()

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

    def register_task(self, task_id, initial_percent=0):
        with self.tasks_lock:
            self.active_tasks[task_id] = {
                "status": "running",
                "percent": initial_percent,
                "logs": [],
                "result": None,
                "error": None
            }

    def update_task(self, task_id, percent=None, log_msg=None, status=None, result=None, error=None):
        with self.tasks_lock:
            if task_id not in self.active_tasks:
                return
            task = self.active_tasks[task_id]
            if percent is not None:
                task["percent"] = percent
            if log_msg is not None:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                task["logs"].append(f"[{timestamp}] {log_msg}")
            if status is not None:
                task["status"] = status
            if result is not None:
                task["result"] = result
            if error is not None:
                task["error"] = error

    def get_task_status(self, task_id, last_log_idx):
        with self.tasks_lock:
            if task_id not in self.active_tasks:
                return {"success": False, "error": "Taak niet gevonden."}
            task = self.active_tasks[task_id]
            logs = task["logs"][last_log_idx:]
            return {
                "success": True,
                "status": task["status"],
                "percent": task["percent"],
                "new_logs": logs,
                "result": task["result"] if task["status"] == "completed" else None,
                "error": task["error"]
            }

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
            self.is_addon = True
            
            # Gebruik /data/config.yaml voor persistentie in addon-modus
            addon_config_path = "/data/config.yaml"
            if not os.path.exists(addon_config_path):
                logger.info(f"Geen persistente config.yaml gevonden op {addon_config_path}. Initialiseren met standaard config...")
                try:
                    import shutil
                    if os.path.exists(self.config_path):
                        shutil.copy(self.config_path, addon_config_path)
                    elif os.path.exists("config.yaml"):
                        shutil.copy("config.yaml", addon_config_path)
                except Exception as e:
                    logger.error(f"Fout bij kopiëren van standaard config.yaml naar /data: {e}")
            
            self.config_path = addon_config_path
            
            try:
                with open("/data/options.json", 'r', encoding='utf-8') as f:
                    ha_options = json.load(f)
                
                # Probeer de persistente config.yaml te laden
                local_cfg = {}
                if os.path.exists(self.config_path):
                    try:
                        with open(self.config_path, 'r', encoding='utf-8') as cf:
                            local_cfg = yaml.safe_load(cf) or {}
                        logger.info(f"Persistente config.yaml succesvol geladen van {self.config_path}")
                    except Exception as e:
                        logger.error(f"Fout bij laden van {self.config_path}: {e}")

                # Combineer HA-addon opties met de persistente config.yaml (prioriteit voor config.yaml)
                local_mqtt = local_cfg.get("mqtt", {})
                local_modbus = local_cfg.get("modbus", {})
                
                self.config = {
                    "mqtt": {
                        "broker": local_mqtt.get("broker") or ha_options.get("mqtt_broker", "localhost"),
                        "port": local_mqtt.get("port") or ha_options.get("mqtt_port", 1883),
                        "username": local_mqtt.get("username") or ha_options.get("mqtt_username"),
                        "password": local_mqtt.get("password") or ha_options.get("mqtt_password"),
                        "topic_prefix": local_mqtt.get("topic_prefix") or ha_options.get("mqtt_topic_prefix", "usr_n580_bridge"),
                        "discovery_prefix": local_mqtt.get("discovery_prefix") or ha_options.get("mqtt_discovery_prefix", "homeassistant"),
                        "client_id": local_mqtt.get("client_id", "modbus_mqtt_bridge"),
                        "keepalive": local_mqtt.get("keepalive", 60)
                    },
                    "modbus": {
                        "host": local_modbus.get("host") or ha_options.get("modbus_host", "192.168.50.96"),
                        "port": local_modbus.get("port") or ha_options.get("modbus_port", 41),
                        "timeout": local_modbus.get("timeout", 3),
                        "poll_interval": local_modbus.get("poll_interval") or ha_options.get("poll_interval", 15),
                        "retries": local_modbus.get("retries") or ha_options.get("retries", 3),
                        "delay_between_requests": local_modbus.get("delay_between_requests") or ha_options.get("delay_between_requests", 0.1),
                        "max_gap": local_modbus.get("max_gap") or ha_options.get("max_gap", 10),
                        "slave_retries": local_modbus.get("slave_retries") or ha_options.get("slave_retries", {}),
                        "slave_max_gaps": local_modbus.get("slave_max_gaps") or {},
                        "slave_delays": local_modbus.get("slave_delays") or {}
                    },
                    "log_level": local_cfg.get("log_level") or ha_options.get("log_level", "info"),
                    "sensors": local_cfg.get("sensors") or ha_options.get("sensors", [])
                }
                
                logger.info("Addon opties succesvol ingeladen en getransformeerd met persistentie!")
            except Exception as e:
                logger.error(f"Fout bij verwerken van Home Assistant opties: {e}")
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

        # Zorg voor veilige defaults voor de geavanceerde Modbus-instellingen
        modbus_cfg = self.config.setdefault("modbus", {})
        modbus_cfg.setdefault("retries", 3)
        modbus_cfg.setdefault("delay_between_requests", 0.1)
        modbus_cfg.setdefault("max_gap", 10)
        modbus_cfg.setdefault("slave_retries", {})
        modbus_cfg.setdefault("slave_max_gaps", {})
        modbus_cfg.setdefault("slave_delays", {})

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

        # Groepeer sensoren voor block reading
        self.group_sensors()

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
        with self.modbus_lock:
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
                    
                max_retries = self.get_slave_retries(slave)
                delay = float(self.config.get("modbus", {}).get("delay_between_requests", 0.1))
                res = None
                attempt = 0
                while attempt <= max_retries:
                    if attempt > 0:
                        modbus_logger.info(f"Retry {attempt}/{max_retries} voor schrijven naar register {address} op slave {slave} met waarde {val}...")
                        time.sleep(delay)
                    try:
                        res = self.modbus_client.write_register(address=address, value=val, device_id=slave)
                        if res is not None and not res.isError():
                            break
                    except Exception as e:
                        modbus_logger.error(f"Fout bij schrijven naar Modbus (poging {attempt}): {e}")
                        res = None
                    attempt += 1

                if res is None or res.isError():
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
        with self.modbus_lock:
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
    def group_sensors(self):
        """
        Groepeer sensoren in optimale Modbus-blokken per slave en register_type.
        Dit vermindert het aantal polling verzoeken drastisch.
        """
        sensors = self.config.get("sensors", [])
        if not sensors:
            self.sensor_blocks = []
            return

        modbus_cfg = self.config.get("modbus", {})
        global_max_gap = int(modbus_cfg.get("max_gap", 10))
        slave_max_gaps = modbus_cfg.get("slave_max_gaps", {})

        # Groepeer sensoren per (slave, register_type)
        grouped = {}
        for s in sensors:
            slave = int(s.get("slave", 1))
            reg_type = s.get("register_type", "holding")
            key = (slave, reg_type)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(s)

        self.sensor_blocks = []

        for (slave, reg_type), slave_sensors in grouped.items():
            # Bepaal max_gap voor deze specifieke slave
            slave_gap = slave_max_gaps.get(str(slave))
            if slave_gap is None:
                slave_gap = slave_max_gaps.get(int(slave))
            if slave_gap is None:
                slave_gap = global_max_gap
            else:
                slave_gap = int(slave_gap)

            sensor_ranges = []
            for s in slave_sensors:
                addr = int(s.get("address", 0))
                count = 2 if s.get("data_type") in ("int32", "uint32") else 1
                sensor_ranges.append({
                    "sensor": s,
                    "start": addr,
                    "end": addr + count - 1,
                    "count": count
                })

            # Sorteer op start adres
            sensor_ranges.sort(key=lambda x: x["start"])

            # Voeg samen tot blokken met een maximale kloof van `slave_gap` en max block size van 120 registers
            current_block = None
            for r in sensor_ranges:
                if current_block is None:
                    current_block = {
                        "slave": slave,
                        "register_type": reg_type,
                        "start": r["start"],
                        "end": r["end"],
                        "sensors": [r["sensor"]]
                    }
                else:
                    gap = r["start"] - current_block["end"] - 1
                    new_size = r["end"] - current_block["start"] + 1
                    
                    if gap <= slave_gap and new_size <= 120:
                        current_block["end"] = max(current_block["end"], r["end"])
                        current_block["sensors"].append(r["sensor"])
                    else:
                        self.sensor_blocks.append(current_block)
                        current_block = {
                            "slave": slave,
                            "register_type": reg_type,
                            "start": r["start"],
                            "end": r["end"],
                            "sensors": [r["sensor"]]
                        }
            if current_block:
                self.sensor_blocks.append(current_block)

        logger.info(f"Sensoren gegroepeerd in {len(self.sensor_blocks)} Modbus-blokken (Totaal: {len(sensors)} sensoren).")

    def get_slave_retries(self, slave):
        """Haal het aantal toegestane retries op voor een specifiek Slave ID."""
        modbus_cfg = self.config.get("modbus", {})
        slave_retries = modbus_cfg.get("slave_retries", {})
        
        ret = slave_retries.get(str(slave))
        if ret is None:
            ret = slave_retries.get(int(slave))
        if ret is None:
            ret = modbus_cfg.get("retries", 3)
        return int(ret)

    def read_block(self, block):
        """Lees een heel Modbus-blok uit."""
        with self.modbus_lock:
            slave = block["slave"]
            reg_type = block["register_type"]
            start_addr = block["start"]
            count = block["end"] - start_addr + 1

            modbus_logger.debug(f"Blok inlezen (Slave: {slave}, Type: {reg_type}, Adres: {start_addr}, Aantal: {count})")

            # Modbus verbinding controleren/herstellen
            if not self.modbus_client or not self.modbus_client.connected:
                modbus_logger.warning("Modbus client niet verbonden. Poging tot herverbinden...")
                if not self.connect_modbus():
                    return None

            try:
                if reg_type == "holding":
                    res = self.modbus_client.read_holding_registers(address=start_addr, count=count, device_id=slave)
                elif reg_type == "input":
                    res = self.modbus_client.read_input_registers(address=start_addr, count=count, device_id=slave)
                else:
                    modbus_logger.error(f"Onbekend register type: {reg_type} voor blok")
                    return None

                if res.isError():
                    modbus_logger.error(f"Fout bij uitlezen blok (Slave {slave}, Adres {start_addr}, Aantal {count}): {res}")
                    if self.modbus_client:
                        self.modbus_client.close()
                    return None

                return res.registers

            except Exception as e:
                modbus_logger.error(f"Exception bij uitlezen blok: {e}")
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
        """Lees alle sensoren uit via geoptimaliseerde blokken met fallback."""
        if True:
            if not hasattr(self, "sensor_blocks") or not self.sensor_blocks:
                self.group_sensors()
                if not self.sensor_blocks:
                    logger.warning("Geen sensoren geconfigureerd om te polliceren.")
                    return

            modbus_cfg = self.config.get("modbus", {})
            delay = float(modbus_cfg.get("delay_between_requests", 0.1))
            
            self.stats["total_polls"] += 1
            logger.info(f"Starten pollingronde via {len(self.sensor_blocks)} Modbus-blokken...")

            failed_slaves = set()
            success_count = 0
            slaves_marked_online = set()

            for block in self.sensor_blocks:
                slave = block["slave"]
                reg_type = block["register_type"]
                start_addr = block["start"]
                block_sensors = block["sensors"]

                # Sla het blok over als deze slave al gemarkeerd is als gefaald in deze ronde
                if slave in failed_slaves:
                    self.stats["failed_reads"] += len(block_sensors)
                    continue

                max_retries = self.get_slave_retries(slave)
                
                # Bepaal delay voor deze specifieke slave
                slave_delays = modbus_cfg.get("slave_delays", {})
                slave_delay = slave_delays.get(str(slave))
                if slave_delay is None:
                    slave_delay = slave_delays.get(int(slave))
                if slave_delay is None:
                    slave_delay = delay
                else:
                    slave_delay = float(slave_delay)

                registers = None
                attempt = 0
                
                while attempt <= max_retries:
                    if attempt > 0:
                        modbus_logger.info(f"Retry {attempt}/{max_retries} voor Blok (Slave {slave}, Adres {start_addr})...")
                        time.sleep(slave_delay)

                    registers = self.read_block(block)
                    if registers is not None:
                        break
                    attempt += 1

                if registers is not None:
                    for s in block_sensors:
                        uid = s.get("unique_id")
                        addr = int(s.get("address", 0))
                        count = 2 if s.get("data_type") in ("int32", "uint32") else 1
                        
                        offset = addr - start_addr
                        
                        if offset >= 0 and offset + count <= len(registers):
                            sensor_regs = registers[offset : offset + count]
                            
                            if count == 2:
                                raw_value = (sensor_regs[0] << 16) | sensor_regs[1]
                                if s.get("data_type") == "int32" and raw_value >= 2147483648:
                                    raw_value -= 4294967296
                                debug_raw = f"{sensor_regs[0]},{sensor_regs[1]} ({raw_value})"
                            else:
                                raw_value = sensor_regs[0]
                                if s.get("data_type", "int16") == "int16" and raw_value >= 32768:
                                    raw_value -= 65536
                                debug_raw = str(sensor_regs[0])

                            scale = s.get("scale", 1.0)
                            precision = s.get("precision", 0)
                            calculated_value = round(raw_value * scale, precision) if precision > 0 else int(round(raw_value * scale))
                            
                            modbus_logger.debug(f"Gelezen {uid} via blok: Raw={debug_raw} -> Berekend={calculated_value}")
                            self.publish_sensor_value(uid, calculated_value)
                            success_count += 1
                            self.stats["successful_reads"] += 1
                        else:
                            modbus_logger.error(f"Offset buiten bereik voor sensor {uid} in blok: offset={offset}, len={len(registers)}")
                            self.stats["failed_reads"] += 1

                    if slave not in slaves_marked_online:
                        self.publish_slave_connectivity(slave, "ON")
                        slaves_marked_online.add(slave)
                else:
                    modbus_logger.warning(f"Blok uitlezen mislukt voor Slave {slave}, Adres {start_addr}. Fallback naar individuele sensoren...")
                    
                    block_failed = False
                    for s in block_sensors:
                        uid = s.get("unique_id")
                        
                        val = None
                        indiv_attempt = 0
                        while indiv_attempt <= max_retries:
                            if indiv_attempt > 0:
                                time.sleep(slave_delay)
                            val = self.read_sensor(s)
                            if val is not None:
                                break
                            indiv_attempt += 1

                        if val is not None:
                            self.publish_sensor_value(uid, val)
                            success_count += 1
                            self.stats["successful_reads"] += 1
                        else:
                            block_failed = True
                            self.stats["failed_reads"] += 1

                    if block_failed:
                        failed_slaves.add(slave)
                        self.publish_slave_connectivity(slave, "OFF")
                        modbus_logger.warning(f"Slave {slave} reageert niet na individuele fallback. Resterende blokken voor deze slave worden overgeslagen.")
                    else:
                        if slave not in slaves_marked_online:
                            self.publish_slave_connectivity(slave, "ON")
                            slaves_marked_online.add(slave)

                time.sleep(slave_delay)

            logger.info(f"Pollingronde voltooid. {success_count} metingen succesvol verwerkt.")

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
        # Lees versie dynamisch uit de addon config.yaml
        version = "?"
        try:
            import re
            addon_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
            if os.path.exists(addon_cfg):
                with open(addon_cfg, 'r') as f:
                    m = re.search(r'^version:\s*["\']?([\d.]+)["\']?', f.read(), re.MULTILINE)
                    if m:
                        version = m.group(1)
        except Exception:
            pass
        return HTML_TEMPLATE.replace("{{INGRESS_PATH}}", ingress_path).replace("{{VERSION}}", version)

    def run_slave_benchmark(self, slave_id, task_id=None):
        """Voert een uitgebreide stress-test uit op een specifieke slave om de optimale grouping en delay te bepalen."""
        sensors = self.config.get("sensors", [])
        slave_sensors = [s for s in sensors if int(s.get("slave", 1)) == slave_id]
        if not slave_sensors:
            err = f"Geen sensoren geconfigureerd voor Slave {slave_id}."
            if task_id:
                self.update_task(task_id, status="failed", error=err)
            return {"success": False, "error": err}

        msg_start = f"🏁 Starten van Modbus stress-test en optimalisatie voor Slave {slave_id} (Sensoren: {len(slave_sensors)})..."
        logger.info(msg_start)
        if task_id:
            self.update_task(task_id, percent=5, log_msg=msg_start)

        # Test combinaties van max_gap en delay
        test_gaps = [1, 2, 5, 10, 15, 20]
        test_delays = [0.0, 0.02, 0.05, 0.1, 0.15]

        best_cfg = None
        best_duration = float('inf')
        best_success_rate = 0.0
        results = []

        with self.modbus_lock:
            # Sorteer sensoren op start-adres
            sensor_ranges = []
            for s in slave_sensors:
                addr = int(s.get("address", 0))
                count = 2 if s.get("data_type") in ("int32", "uint32") else 1
                sensor_ranges.append({
                    "sensor": s,
                    "start": addr,
                    "end": addr + count - 1,
                    "count": count
                })
            sensor_ranges.sort(key=lambda x: x["start"])

            total_combos = len(test_gaps) * len(test_delays)
            combo_idx = 0

            for gap in test_gaps:
                # Groepeer sensoren specifiek voor deze max_gap test
                blocks = []
                current_block = None
                for r in sensor_ranges:
                    if current_block is None:
                        current_block = {
                            "slave": slave_id,
                            "register_type": r["sensor"].get("register_type", "holding"),
                            "start": r["start"],
                            "end": r["end"],
                            "sensors": [r["sensor"]]
                        }
                    else:
                        reg_type = r["sensor"].get("register_type", "holding")
                        g = r["start"] - current_block["end"] - 1
                        new_size = r["end"] - current_block["start"] + 1

                        if reg_type == current_block["register_type"] and g <= gap and new_size <= 120:
                            current_block["end"] = max(current_block["end"], r["end"])
                            current_block["sensors"].append(r["sensor"])
                        else:
                            blocks.append(current_block)
                            current_block = {
                                "slave": slave_id,
                                "register_type": reg_type,
                                "start": r["start"],
                                "end": r["end"],
                                "sensors": [r["sensor"]]
                            }
                if current_block:
                    blocks.append(current_block)

                # Test deze groepering met verschillende delays
                for delay in test_delays:
                    percent = 5 + int(85 * (combo_idx / total_combos))
                    combo_idx += 1
                    
                    msg_test = f"Combinatie {combo_idx}/{total_combos} testen: gap={gap}, delay={delay:.2f}s (Aantal blokken: {len(blocks)})..."
                    logger.info(msg_test)
                    if task_id:
                        self.update_task(task_id, percent=percent, log_msg=msg_test)

                    successes = 0
                    failures = 0
                    total_time = 0.0

                    # Doe 3 test runs per configuratie
                    for iteration in range(3):
                        start_run = time.time()
                        run_success = True

                        for block in blocks:
                            res = self.read_block(block)
                            if res is None:
                                run_success = False
                                break
                            if len(blocks) > 1 and delay > 0:
                                time.sleep(delay)

                        duration = time.time() - start_run
                        if run_success:
                            successes += 1
                            total_time += duration
                        else:
                            failures += 1

                    success_rate = successes / 3.0
                    avg_duration = (total_time / successes) if successes > 0 else float('inf')

                    results.append({
                        "max_gap": gap,
                        "delay_between_requests": delay,
                        "success_rate": success_rate,
                        "avg_duration_sec": avg_duration,
                        "blocks_count": len(blocks)
                    })

                    msg_res = f"  -> Resultaat: Success={success_rate * 100}%, Tijd={avg_duration:.3f}s"
                    logger.info(msg_res)
                    if task_id:
                        self.update_task(task_id, log_msg=msg_res)

                    # Bepaal of dit de beste stabiele instelling is
                    if success_rate > best_success_rate:
                        best_success_rate = success_rate
                        best_duration = avg_duration
                        best_cfg = {"max_gap": gap, "delay_between_requests": delay, "blocks_count": len(blocks)}
                    elif success_rate == best_success_rate and success_rate > 0.0:
                        # Als succes rate gelijk is, kies de snelste
                        if avg_duration < best_duration:
                            best_duration = avg_duration
                            best_cfg = {"max_gap": gap, "delay_between_requests": delay, "blocks_count": len(blocks)}

        if best_cfg:
            msg_best = f"🏆 Optimale instelling voor Slave {slave_id}: max_gap={best_cfg['max_gap']}, delay={best_cfg['delay_between_requests']} (Succes: {best_success_rate * 100}%, Tijd: {best_duration:.3f}s)"
            logger.info(msg_best)
            ret = {
                "success": True,
                "slave": slave_id,
                "optimal_settings": best_cfg,
                "best_success_rate": best_success_rate,
                "best_duration_sec": best_duration,
                "all_runs": results
            }
            if task_id:
                self.update_task(task_id, percent=100, log_msg=msg_best, status="completed", result=ret)
            return ret
        else:
            err_fail = f"❌ Geen enkele succesvolle poll-configuratie gevonden tijdens stress-test voor Slave {slave_id}!"
            logger.error(err_fail)
            ret = {
                "success": False,
                "slave": slave_id,
                "error": "Geen enkele configuratie werkte stabiel tijdens de stress-test. Controleer de bekabeling."
            }
            if task_id:
                self.update_task(task_id, percent=100, log_msg=err_fail, status="failed", error=ret["error"])
            return ret

    def apply_settings(self, slave_id, max_gap, delay):
        """Sla de optimale instellingen op voor een specifieke slave in config.yaml."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            config_data = yaml.safe_load(content) or {}
        except Exception as e:
            raise ValueError(f"Kan huidige configuratie niet inlezen: {e}")

        modbus_cfg = config_data.setdefault("modbus", {})
        slave_max_gaps = modbus_cfg.setdefault("slave_max_gaps", {})
        slave_delays = modbus_cfg.setdefault("slave_delays", {})

        # Sla de parameters op
        slave_max_gaps[str(slave_id)] = int(max_gap)
        slave_delays[str(slave_id)] = float(delay)

        # Dump terug naar YAML met behoud van opmaak (Unicode)
        try:
            new_yaml = yaml.dump(config_data, allow_unicode=True, sort_keys=False)
            self.reload_configuration(new_yaml)
            return {"success": True, "message": f"Instellingen succesvol toegepast en opgeslagen voor Slave {slave_id}!"}
        except Exception as e:
            raise ValueError(f"Fout bij opslaan van nieuwe instellingen: {e}")

    def scan_range(self, slave, reg_type, start_addr, quantity, task_id=None, pct_range=None):
        """Scant een specifieke reeks registers in blokken en valt terug op individueel."""
        results = []
        block_size = 20
        steps = list(range(start_addr, start_addr + quantity, block_size))
        total_steps = len(steps)
        
        for idx, block_start in enumerate(steps):
            block_count = min(block_size, start_addr + quantity - block_start)
            block = {
                "slave": slave,
                "register_type": reg_type,
                "start": block_start,
                "end": block_start + block_count - 1
            }
            
            if task_id and pct_range:
                start_pct, end_pct = pct_range
                curr_pct = int(start_pct + (end_pct - start_pct) * (idx / total_steps))
                self.update_task(task_id, percent=curr_pct)
            
            msg = f"Scan block {block_start} t/m {block_start + block_count - 1} op type '{reg_type}'..."
            if task_id:
                self.update_task(task_id, log_msg=msg)
            logger.info(msg)
            
            regs = self.read_block(block)
            if regs is not None:
                msg_ok = f"  -> Gevonden: {len(regs)} registers"
                if task_id:
                    self.update_task(task_id, log_msg=msg_ok)
                logger.info(msg_ok)
                for i in range(len(regs)):
                    results.append({
                        "address": block_start + i,
                        "register_type": reg_type,
                        "value_16": regs[i]
                    })
            else:
                msg_fail = f"  -> Mislukt, proberen per register..."
                if task_id:
                    self.update_task(task_id, log_msg=msg_fail)
                logger.warning(msg_fail)
                for addr in range(block_start, block_start + block_count):
                    single_block = {
                        "slave": slave,
                        "register_type": reg_type,
                        "start": addr,
                        "end": addr
                    }
                    val = self.read_block(single_block)
                    if val is not None and len(val) > 0:
                        msg_sing = f"    Register {addr}: {val[0]}"
                        if task_id:
                            self.update_task(task_id, log_msg=msg_sing)
                        results.append({
                            "address": addr,
                            "register_type": reg_type,
                            "value_16": val[0]
                        })
                    time.sleep(0.01)
        return results

    def scan_registers(self, slave_id, register_type, start_addr, quantity, task_id=None):
        """Scant holding en/of input registers op een slave met de modbus lock."""
        slave_id = int(slave_id)
        start_addr = int(start_addr)
        quantity = int(quantity)
        
        if quantity <= 0 or quantity > 1000:
            if task_id:
                self.update_task(task_id, status="failed", error="Aantal te scannen registers moet tussen 1 en 1000 liggen.")
            return {"success": False, "error": "Aantal te scannen registers moet tussen 1 en 1000 liggen."}
            
        msg = f"🏁 Starten Modbus register scan op Slave {slave_id} (Type: {register_type}, Bereik: {start_addr}-{start_addr+quantity-1})..."
        logger.info(msg)
        if task_id:
            self.update_task(task_id, percent=5, log_msg=msg)
        
        raw_results = []
        types_to_scan = []
        if register_type in ("holding", "both"):
            types_to_scan.append("holding")
        if register_type in ("input", "both"):
            types_to_scan.append("input")
            
        with self.modbus_lock:
            for type_idx, reg_type in enumerate(types_to_scan):
                try:
                    pct_range = (5 + type_idx * 40, 5 + (type_idx + 1) * 40)
                    type_results = self.scan_range(slave_id, reg_type, start_addr, quantity, task_id, pct_range)
                    raw_results.extend(type_results)
                except Exception as e:
                    err_msg = f"Fout tijdens scan op {reg_type} registers: {e}"
                    logger.error(err_msg)
                    if task_id:
                        self.update_task(task_id, log_msg=f"❌ {err_msg}")
                    
        # Bouw een dictionary van (reg_type, adres) -> 16-bit waarde voor snelle lookup
        vals_map = {(r["register_type"], r["address"]): r["value_16"] for r in raw_results}
        
        if task_id:
            self.update_task(task_id, percent=90, log_msg="Gelezen data structureren...")
            
        results = []
        for r in raw_results:
            addr = r["address"]
            reg_type = r["register_type"]
            val16 = r["value_16"]
            
            val_int16 = val16 - 65536 if val16 >= 32768 else val16
            val_uint16 = val16
            
            item = {
                "address": addr,
                "register_type": reg_type,
                "val_int16": val_int16,
                "val_uint16": val_uint16
            }
            
            # Controleer of het opvolgende register ook succesvol gelezen is (voor 32-bit datatypes)
            if (reg_type, addr + 1) in vals_map:
                val16_next = vals_map[(reg_type, addr + 1)]
                val32 = (val16 << 16) | val16_next
                val_int32 = val32 - 4294967296 if val32 >= 2147483648 else val32
                val_uint32 = val32
                item["val_int32"] = val_int32
                item["val_uint32"] = val_uint32
                
            results.append(item)
            
        msg_fin = f"🔍 Scan voltooid op Slave {slave_id}. {len(results)} actieve registers gevonden."
        logger.info(msg_fin)
        ret = {
            "success": True,
            "slave": slave_id,
            "results": results
        }
        if task_id:
            self.update_task(task_id, percent=100, log_msg=msg_fin, status="completed", result=ret)
        return ret

    def add_sensors(self, new_sensors):
        """Voegt een lijst met gescande sensoren toe aan config.yaml en herlaadt."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            config_data = yaml.safe_load(content) or {}
        except Exception as e:
            raise ValueError(f"Kan huidige configuratie niet inlezen: {e}")

        sensors_list = config_data.setdefault("sensors", [])
        existing_ids = {s.get("unique_id") for s in sensors_list if s.get("unique_id")}
        
        for s in new_sensors:
            uid = s.get("unique_id")
            if not uid:
                raise ValueError("Elke sensor moet een unique_id hebben.")
            if uid in existing_ids:
                raise ValueError(f"Sensor met unique_id '{uid}' bestaat al.")
            
            sensor_entry = {
                "name": str(s.get("name")),
                "unique_id": str(uid),
                "slave": int(s.get("slave", 1)),
                "register_type": str(s.get("register_type", "holding")),
                "address": int(s.get("address", 0)),
                "data_type": str(s.get("data_type", "int16")),
                "scale": float(s.get("scale", 1.0)),
                "precision": int(s.get("precision", 0)),
                "unit_of_measurement": str(s.get("unit_of_measurement", "")),
                "device_class": str(s.get("device_class", "")),
                "state_class": str(s.get("state_class", "measurement"))
            }
            
            if s.get("writeable"):
                sensor_entry["writeable"] = True
                
            if s.get("entity_type") and s.get("entity_type") != "sensor":
                sensor_entry["entity_type"] = str(s.get("entity_type"))
                if sensor_entry["entity_type"] == "number":
                    sensor_entry["min"] = s.get("min", 0)
                    sensor_entry["max"] = s.get("max", 100)
                    sensor_entry["step"] = s.get("step", 1)

            sensors_list.append(sensor_entry)

        try:
            new_yaml = yaml.dump(config_data, allow_unicode=True, sort_keys=False)
            self.reload_configuration(new_yaml)
            return {"success": True, "message": f"{len(new_sensors)} sensoren succesvol toegevoegd en hergeladen!"}
        except Exception as e:
            raise ValueError(f"Fout bij opslaan van nieuwe sensoren: {e}")

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
class StatusHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bridge):
        super().__init__(server_address, RequestHandlerClass)
        self.bridge = bridge


class StatusHTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Log HTTP-verzoeken op INFO niveau voor debug doeleinden
        logger.info(f"[HTTP] {format % args}")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Ingress-Path")
        self.end_headers()

    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query = parse_qs(parsed_url.query)
        
        # Log binnenkomende headers voor diagnose
        ingress_path = self.headers.get("X-Ingress-Path", "")
        ingress_path_raw = self.headers.get("X-Ingress-Path", "NIET_AANWEZIG")
        logger.info(f"[HTTP-DEBUG] GET {path} | X-Ingress-Path={ingress_path_raw!r} | Host={self.headers.get('Host','?')!r}")
        
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
        elif path.endswith("/api/task-status"):
            task_id = query.get("task_id", [None])[0]
            last_log_idx = int(query.get("last_log_index", [0])[0])
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            status_data = self.server.bridge.get_task_status(task_id, last_log_idx)
            self.wfile.write(json.dumps(status_data).encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
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
                self.wfile.write(json.dumps({"success": True, "message": "Configurature succesvol bijgewerkt en herladen!"}).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode('utf-8'))
        elif path.endswith("/api/optimize"):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                params = json.loads(post_data)
                slave = int(params.get("slave"))
                
                task_id = f"optimize_slave_{slave}_{int(time.time() * 1000)}"
                self.server.bridge.register_task(task_id)
                
                t = threading.Thread(
                    target=self.server.bridge.run_slave_benchmark,
                    args=(slave, task_id),
                    daemon=True
                )
                t.start()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "task_id": task_id}).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode('utf-8'))
        elif path.endswith("/api/apply-settings"):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                params = json.loads(post_data)
                slave = int(params.get("slave"))
                max_gap = int(params.get("max_gap"))
                delay = float(params.get("delay"))
                
                result = self.server.bridge.apply_settings(slave, max_gap, delay)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode('utf-8'))
        elif path.endswith("/api/scan-registers"):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                params = json.loads(post_data)
                slave = int(params.get("slave"))
                reg_type = str(params.get("register_type", "holding"))
                start_addr = int(params.get("start_address", 0))
                quantity = int(params.get("quantity", 100))
                
                task_id = f"scan_slave_{slave}_{int(time.time() * 1000)}"
                self.server.bridge.register_task(task_id)
                
                t = threading.Thread(
                    target=self.server.bridge.scan_registers,
                    args=(slave, reg_type, start_addr, quantity, task_id),
                    daemon=True
                )
                t.start()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "task_id": task_id}).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode('utf-8'))
        elif path.endswith("/api/add-sensors"):
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                params = json.loads(post_data)
                new_sensors = params.get("sensors", [])
                
                if not isinstance(new_sensors, list) or not new_sensors:
                    raise ValueError("Ongeldige of lege lijst met sensoren.")
                
                result = self.server.bridge.add_sensors(new_sensors)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode('utf-8'))
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
        
        .log-console {
            background-color: rgba(0, 0, 0, 0.4);
            border: 1px solid var(--card-border);
            border-radius: 8px;
            padding: 0.75rem;
            margin-top: 1rem;
            max-height: 200px;
            overflow-y: auto;
            font-family: 'Courier New', Courier, monospace;
            font-size: 0.85rem;
            color: #10b981;
            white-space: pre-wrap;
            word-break: break-all;
            text-align: left;
            display: none;
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
            <button id="btn-tab-optimizer" class="tab-btn" onclick="switchTab('optimizer')">⚙️ Optimalisatie</button>
            <button id="btn-tab-scanner" class="tab-btn" onclick="switchTab('scanner')">🔍 Register Scanner</button>
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

        <!-- Tab 3: Optimizer -->
        <div id="tab-optimizer" class="tab-content">
            <div class="editor-container">
                <div class="section-title">⚙️ Modbus Slave Stress-test & Optimalisatie</div>
                <p style="color: var(--text-muted); margin-bottom: 1.5rem; font-size: 0.95rem;">
                    Hier kun je per aangesloten Modbus-apparaat (Slave ID) een intensieve stresstest uitvoeren. Het systeem test verschillende register-blokgroottes (`max_gap`) en wachttijden (`delay_between_requests`) om de snelste, 100% stabiele instelling te bepalen.
                </p>
                <div id="optimizer-list" class="grid" style="grid-template-columns: 1fr;">
                    <div class="card">
                        <div class="text-center">Gegevens laden...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab 4: Register Scanner -->
        <div id="tab-scanner" class="tab-content">
            <div class="editor-container">
                <div class="section-title">🔍 Modbus Register Scanner</div>
                <p style="color: var(--text-muted); margin-bottom: 1.5rem; font-size: 0.95rem;">
                    Scan een reeks registers op een specifieke Modbus slave om actieve adressen op te sporen en voeg ze rechtstreeks toe aan je sensorenlijst.
                </p>
                <div class="card" style="margin-bottom: 1.5rem;">
                    <div style="display: flex; gap: 1rem; flex-wrap: wrap; align-items: flex-end;">
                        <div style="flex: 1; min-width: 120px;">
                            <label style="display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: var(--text-muted);">Slave ID</label>
                            <input type="number" id="scan-slave-id" value="1" min="1" max="255" style="width: 100%; padding: 0.5rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 8px; color: #fff; outline: none;">
                        </div>
                        <div style="flex: 2; min-width: 180px;">
                            <label style="display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: var(--text-muted);">Register Type</label>
                            <select id="scan-register-type" style="width: 100%; padding: 0.5rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 8px; color: #fff; outline: none;">
                                <option value="holding">Holding Registers</option>
                                <option value="input">Input Registers</option>
                                <option value="both">Beide Types</option>
                            </select>
                        </div>
                        <div style="flex: 1.5; min-width: 120px;">
                            <label style="display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: var(--text-muted);">Start Adres</label>
                            <input type="number" id="scan-start-addr" value="0" min="0" max="65535" style="width: 100%; padding: 0.5rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 8px; color: #fff; outline: none;">
                        </div>
                        <div style="flex: 1.5; min-width: 120px;">
                            <label style="display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: var(--text-muted);">Aantal te scannen</label>
                            <input type="number" id="scan-quantity" value="100" min="1" max="1000" style="width: 100%; padding: 0.5rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 8px; color: #fff; outline: none;">
                        </div>
                        <div>
                            <button class="btn btn-primary" id="btn-start-scan" onclick="startRegisterScan()">⚡ Start Scan</button>
                        </div>
                    </div>
                    
                    <!-- Scan Voortgangsindicator -->
                    <div id="scan-progress-container" style="display: none; margin-top: 1.5rem; border-top: 1px solid var(--card-border); padding-top: 1rem;">
                        <div style="display: flex; justify-content: space-between; font-size: 0.9rem; margin-bottom: 0.5rem;">
                            <span id="scan-status-text" style="color: var(--primary); font-weight: 600;">Bezig met scannen...</span>
                            <span id="scan-percent-text">0%</span>
                        </div>
                        <div style="width: 100%; height: 8px; background: rgba(255,255,255,0.05); border-radius: 4px; overflow: hidden; border: 1px solid var(--card-border); margin-bottom: 1rem;">
                            <div id="scan-progress-bar" style="width: 0%; height: 100%; background: var(--primary); transition: width 0.2s;"></div>
                        </div>
                        <div id="scan-log-console" class="log-console"></div>
                    </div>
                </div>

                <!-- Scan Resultaten -->
                <div id="scan-results-container" style="display: none;">
                    <div class="section-title">🔍 Gevonden Actieve Registers</div>
                    <div id="scan-alert-container"></div>
                    <table style="margin-bottom: 1.5rem;">
                        <thead>
                            <tr>
                                <th style="width: 40px; text-align: center;"><input type="checkbox" id="scan-select-all" onclick="toggleSelectAllScan(this)"></th>
                                <th>Adres</th>
                                <th>Type</th>
                                <th>Gevonden Waarde (Verschillende Datatypes)</th>
                            </tr>
                        </thead>
                        <tbody id="scan-table-body">
                            <!-- Dynamisch ingevuld -->
                        </tbody>
                    </table>
                    
                    <!-- Dynamic Configurations Form for selected registers -->
                    <div id="scan-configs-container" style="display: none; margin-bottom: 1.5rem;">
                        <div class="section-title">✏️ Configureer Geselecteerde Sensoren</div>
                        <div id="scan-config-forms-list">
                            <!-- Hier komen de configuratie formulieren per geselecteerd register -->
                        </div>
                        
                        <div style="display: flex; justify-content: flex-end; margin-top: 1.5rem;">
                            <button class="btn btn-primary" id="btn-save-scanned" onclick="saveScannedSensors()" style="background-color: var(--success);">
                                ➕ Voeg geselecteerde sensoren toe aan configuratie
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            Modbus-MQTT Bridge Add-on v{{VERSION}} • Ontwikkeld voor Raspberry Pi &amp; Home Assistant
        </div>
    </div>

    <script>
        // Bereken de basis-URL vanuit de huidig geladen pagina-URL
        // HA Ingress serveert de pagina op /api/hassio_ingress/TOKEN/
        // Relatieve URLs t.o.v. de paginabasis werken automatisch correct
        function getApiBase() {
            // Haal de URL op waarvandaan de pagina geladen is
            const pageUrl = window.location.href;
            // Verwijder alles na de laatste / om de basis te krijgen
            const base = pageUrl.replace(/\/[^\/]*(\?.*)?$/, '/');
            // Controleer of dit een HA ingress URL is
            if (base.includes('/api/hassio_ingress/')) {
                return base.replace(/\/$/, '');  // zonder trailing slash
            }
            // Fallback: gebruik server-side injectie
            return "{{INGRESS_PATH}}";
        }
        
        const ingressPath = getApiBase();
        const apiURL = ingressPath + "/api/status";
        const configURL = ingressPath + "/api/config";
        
        let activeTab = 'dashboard';
        let optimizerSlavesRendered = false;
        
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
            } else if (tabId === 'optimizer') {
                fetch(apiURL)
                    .then(response => response.json())
                    .then(data => {
                        buildOptimizerTab(data.sensors);
                    })
                    .catch(err => console.error("Fout bij laden slaves voor optimizer:", err));
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
                    optimizerSlavesRendered = false;
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
        
        function getDeviceName(slaveId, sensors) {
            if (slaveId === 1) return "Warmtepomp";
            if (slaveId === 2) return "Sinotimer";
            if (slaveId >= 3 && slaveId <= 5) return `KWS Verbruiksmeter (Slave ${slaveId})`;
            if (slaveId >= 51 && slaveId <= 55) return `Growatt Zonnepaneel Inverter (Slave ${slaveId})`;
            
            const slaveSensors = sensors.filter(s => parseInt(s.slave) === slaveId);
            if (slaveSensors.length > 0) {
                const names = slaveSensors.map(s => s.name.split(' ')[0]);
                if (names.length > 0) {
                    return names[0] + ` (Slave ${slaveId})`;
                }
            }
            return `Modbus Apparaat (Slave ${slaveId})`;
        }

        function buildOptimizerTab(sensors) {
            if (optimizerSlavesRendered) return;
            
            const slavesMap = {};
            sensors.forEach(s => {
                const slave = parseInt(s.slave);
                if (!slavesMap[slave]) {
                    slavesMap[slave] = {
                        id: slave,
                        sensorCount: 0
                    };
                }
                slavesMap[slave].sensorCount++;
            });
            
            const container = document.getElementById('optimizer-list');
            let html = '';
            
            const sortedSlaves = Object.keys(slavesMap).map(Number).sort((a, b) => a - b);
            
            if (sortedSlaves.length === 0) {
                container.innerHTML = '<div class="card"><div class="text-center">Geen Modbus-slaves gevonden in de configuratie.</div></div>';
                return;
            }
            
            sortedSlaves.forEach(slaveId => {
                const name = getDeviceName(slaveId, sensors);
                const count = slavesMap[slaveId].sensorCount;
                
                html += `
                    <div class="card" id="opt-card-${slaveId}" style="margin-bottom: 1.5rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem;">
                            <div>
                                <h3 style="font-size: 1.25rem; font-weight: 700; margin-bottom: 0.25rem;">${name}</h3>
                                <p style="color: var(--text-muted); font-size: 0.85rem;">
                                    Slave ID: <strong>${slaveId}</strong> • Geconfigureerde sensoren: <strong>${count}</strong>
                                </p>
                            </div>
                            <div>
                                <button class="btn btn-primary" id="btn-opt-${slaveId}" onclick="runStressTest(${slaveId})">
                                    ⚡ Optimaliseer & Stress-test
                                </button>
                            </div>
                        </div>
                        
                        <!-- Voortgangsindicator -->
                        <div id="opt-progress-container-${slaveId}" style="display: none; margin-top: 1.5rem; border-top: 1px solid var(--card-border); padding-top: 1rem;">
                            <div style="display: flex; justify-content: space-between; font-size: 0.9rem; margin-bottom: 0.5rem;">
                                <span id="opt-status-text-${slaveId}" style="color: var(--primary); font-weight: 600;">Bezig met stress-test...</span>
                                <span id="opt-percent-text-${slaveId}">0%</span>
                            </div>
                            <div style="width: 100%; height: 8px; background: rgba(255,255,255,0.05); border-radius: 4px; overflow: hidden; border: 1px solid var(--card-border); margin-bottom: 1rem;">
                                <div id="opt-progress-bar-${slaveId}" style="width: 0%; height: 100%; background: var(--primary); transition: width 0.2s;"></div>
                            </div>
                            <div id="opt-log-console-${slaveId}" class="log-console"></div>
                        </div>
                        
                        <!-- Resultaten weergave -->
                        <div id="opt-results-${slaveId}" style="display: none; margin-top: 1.5rem; border-top: 1px solid var(--card-border); padding-top: 1.25rem;">
                            <!-- Resultaten details komen hier -->
                        </div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
            optimizerSlavesRendered = true;
        }

        function pollTaskStatus(taskId, type, idOrSlaveId, lastLogIdx) {
            const statusURL = `${ingressPath}/api/task-status?task_id=${taskId}&last_log_index=${lastLogIdx}`;
            
            fetch(statusURL)
            .then(response => {
                if (!response.ok) throw new Error("Fout bij ophalen status: " + response.status);
                return response.json();
            })
            .then(result => {
                if (!result.success) {
                    throw new Error(result.error || "Onbekende fout");
                }
                
                let logConsole;
                if (type === 'scanner') {
                    logConsole = document.getElementById('scan-log-console');
                } else {
                    logConsole = document.getElementById(`opt-log-console-${idOrSlaveId}`);
                }
                
                if (result.new_logs && result.new_logs.length > 0) {
                    result.new_logs.forEach(line => {
                        logConsole.innerHTML += line + "\n";
                    });
                    logConsole.scrollTop = logConsole.scrollHeight;
                }
                
                const nextLogIdx = lastLogIdx + (result.new_logs ? result.new_logs.length : 0);
                
                let progressBar, percentText, statusText;
                if (type === 'scanner') {
                    progressBar = document.getElementById('scan-progress-bar');
                    percentText = document.getElementById('scan-percent-text');
                    statusText = document.getElementById('scan-status-text');
                } else {
                    progressBar = document.getElementById(`opt-progress-bar-${idOrSlaveId}`);
                    percentText = document.getElementById(`opt-percent-text-${idOrSlaveId}`);
                    statusText = document.getElementById(`opt-status-text-${idOrSlaveId}`);
                }
                
                progressBar.style.width = `${result.percent}%`;
                percentText.innerText = `${result.percent}%`;
                
                if (type === 'scanner') {
                    statusText.innerText = `Registers scannen (${result.percent}%)...`;
                } else {
                    if (result.percent < 30) {
                        statusText.innerText = "Register-blokgroottes testen (max_gap)...";
                    } else if (result.percent < 60) {
                        statusText.innerText = "Wachttijden scannen (delay_between_requests)...";
                    } else {
                        statusText.innerText = "Optimaliteit en foutmarges berekenen...";
                    }
                }
                
                if (result.status === 'completed') {
                    progressBar.style.width = "100%";
                    percentText.innerText = "100%";
                    
                    if (type === 'scanner') {
                        statusText.innerText = "Scan voltooid!";
                        const btn = document.getElementById('btn-start-scan');
                        btn.disabled = false;
                        btn.innerText = "⚡ Start Scan";
                        
                        const resultsContainer = document.getElementById('scan-results-container');
                        currentScanResults = result.result.results;
                        displayScanResults(result.result.results);
                    } else {
                        statusText.innerText = "Optimalisatie voltooid!";
                        const btn = document.getElementById(`btn-opt-${idOrSlaveId}`);
                        btn.disabled = false;
                        btn.innerText = "⚡ Optimaliseer & Stress-test";
                        
                        displayOptimizationResults(idOrSlaveId, result.result);
                    }
                } else if (result.status === 'failed') {
                    progressBar.style.width = "100%";
                    progressBar.style.backgroundColor = "var(--danger)";
                    statusText.innerText = "Mislukt.";
                    statusText.style.color = "var(--danger)";
                    
                    if (type === 'scanner') {
                        const btn = document.getElementById('btn-start-scan');
                        btn.disabled = false;
                        btn.innerText = "⚡ Start Scan";
                        
                        const resultsContainer = document.getElementById('scan-results-container');
                        const alertContainer = document.getElementById('scan-alert-container');
                        resultsContainer.style.display = "block";
                        alertContainer.innerHTML = `
                            <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                                <strong>❌ Scan mislukt:</strong><br>${result.error || 'Onbekende fout'}
                            </div>
                        `;
                        document.getElementById('scan-table-body').innerHTML = '<tr><td colspan="4" class="text-center">Scan mislukt.</td></tr>';
                        document.getElementById('scan-select-all').checked = false;
                        document.getElementById('scan-configs-container').style.display = "none";
                    } else {
                        const btn = document.getElementById(`btn-opt-${idOrSlaveId}`);
                        btn.disabled = false;
                        btn.innerText = "⚡ Optimaliseer & Stress-test";
                        
                        const resultsContainer = document.getElementById(`opt-results-${idOrSlaveId}`);
                        resultsContainer.innerHTML = `
                            <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                                <strong>❌ Stress-test mislukt:</strong><br>${result.error || 'Onbekende fout'}
                            </div>
                        `;
                        resultsContainer.style.display = "block";
                    }
                } else {
                    setTimeout(() => {
                        pollTaskStatus(taskId, type, idOrSlaveId, nextLogIdx);
                    }, 500);
                }
            })
            .catch(err => {
                console.error("Task status polling error: ", err);
                setTimeout(() => {
                    pollTaskStatus(taskId, type, idOrSlaveId, lastLogIdx);
                }, 1000);
            });
        }

        function runStressTest(slaveId) {
            const btn = document.getElementById(`btn-opt-${slaveId}`);
            btn.disabled = true;
            btn.innerText = "⏳ Bezig met testen...";
            
            const progressContainer = document.getElementById(`opt-progress-container-${slaveId}`);
            const statusText = document.getElementById(`opt-status-text-${slaveId}`);
            const percentText = document.getElementById(`opt-percent-text-${slaveId}`);
            const progressBar = document.getElementById(`opt-progress-bar-${slaveId}`);
            const resultsContainer = document.getElementById(`opt-results-${slaveId}`);
            const logConsole = document.getElementById(`opt-log-console-${slaveId}`);
            
            progressContainer.style.display = "block";
            resultsContainer.style.display = "none";
            logConsole.style.display = "block";
            logConsole.innerHTML = "";
            
            progressBar.style.width = "0%";
            percentText.innerText = "0%";
            statusText.innerText = "Stress-test opstarten op Modbus-lus...";
            
            const optimizeURL = ingressPath + "/api/optimize";
            fetch(optimizeURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ slave: slaveId })
            })
            .then(response => {
                if (!response.ok) throw new Error("Fout status code: " + response.status);
                return response.json();
            })
            .then(result => {
                if (result.success && result.task_id) {
                    pollTaskStatus(result.task_id, 'optimizer', slaveId, 0);
                } else {
                    btn.disabled = false;
                    btn.innerText = "⚡ Optimaliseer & Stress-test";
                    resultsContainer.innerHTML = `
                        <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                            <strong>❌ Stress-test opstarten mislukt:</strong><br>${result.error || 'Onbekende fout'}
                        </div>
                    `;
                    resultsContainer.style.display = "block";
                }
            })
            .catch(err => {
                btn.disabled = false;
                btn.innerText = "⚡ Optimaliseer & Stress-test";
                
                progressBar.style.width = "100%";
                progressBar.style.backgroundColor = "var(--danger)";
                statusText.innerText = "Fout opgetreden.";
                statusText.style.color = "var(--danger)";
                
                resultsContainer.innerHTML = `
                    <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                        <strong>❌ Netwerk- of gatewayfout:</strong><br>${err.message || err}
                    </div>
                `;
                resultsContainer.style.display = "block";
            });
        }

        function displayOptimizationResults(slaveId, result) {
            const resultsContainer = document.getElementById(`opt-results-${slaveId}`);
            resultsContainer.style.display = "block";
            
            const opt = result.optimal_settings;
            const rate = result.best_success_rate * 100;
            const duration = result.best_duration_sec.toFixed(3);
            
            let successColor = "var(--success)";
            if (rate < 100 && rate >= 80) successColor = "var(--warning)";
            if (rate < 80) successColor = "var(--danger)";
            
            resultsContainer.innerHTML = `
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 1.25rem;">
                    <div class="card" style="background: rgba(255,255,255,0.01); border-color: rgba(255,255,255,0.03); padding: 0.75rem 1rem;">
                        <div style="font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;">Geadviseerde Gap</div>
                        <div style="font-size: 1.5rem; font-weight: 800; color: var(--primary);">max_gap: ${opt.max_gap}</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted);">registers maximale kloof</div>
                    </div>
                    <div class="card" style="background: rgba(255,255,255,0.01); border-color: rgba(255,255,255,0.03); padding: 0.75rem 1rem;">
                        <div style="font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;">Geadviseerde Pauze</div>
                        <div style="font-size: 1.5rem; font-weight: 800; color: var(--primary);">${opt.delay_between_requests} s</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted);">delay_between_requests</div>
                    </div>
                    <div class="card" style="background: rgba(255,255,255,0.01); border-color: rgba(255,255,255,0.03); padding: 0.75rem 1rem;">
                        <div style="font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;">Betrouwbaarheid</div>
                        <div style="font-size: 1.5rem; font-weight: 800; color: ${successColor};">${rate.toFixed(1)}%</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted);">Succesvolle test-polls</div>
                    </div>
                    <div class="card" style="background: rgba(255,255,255,0.01); border-color: rgba(255,255,255,0.03); padding: 0.75rem 1rem;">
                        <div style="font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;">Poll Cyclusduur</div>
                        <div style="font-size: 1.5rem; font-weight: 800; color: #fff;">${duration} s</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted);">Groepeert in ${opt.blocks_count} Modbus-blokken</div>
                    </div>
                </div>
                
                <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; background: rgba(59, 130, 246, 0.05); border: 1px dashed rgba(59, 130, 246, 0.2); padding: 1rem; border-radius: 12px;">
                    <div style="font-size: 0.9rem;">
                        <strong>💡 Advies:</strong> Sla deze parameters op om de communicatiesnelheid voor Slave ${slaveId} te optimaliseren.
                    </div>
                    <button class="btn btn-primary" id="btn-apply-${slaveId}" onclick="applyOptimizerSettings(${slaveId}, ${opt.max_gap}, ${opt.delay_between_requests})" style="background-color: var(--success); font-size: 0.9rem; padding: 0.5rem 1.25rem;">
                        ✅ Toepassen & Opslaan
                    </button>
                </div>
            `;
        }

        function applyOptimizerSettings(slaveId, maxGap, delay) {
            const btn = document.getElementById(`btn-apply-${slaveId}`);
            btn.disabled = true;
            btn.innerText = "⏳ Bezig met opslaan...";
            
            const applyURL = ingressPath + "/api/apply-settings";
            fetch(applyURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    slave: slaveId,
                    max_gap: maxGap,
                    delay: delay
                })
            })
            .then(response => {
                if (!response.ok) throw new Error("Fout status code: " + response.status);
                return response.json();
            })
            .then(result => {
                if (result.success) {
                    btn.innerText = "Opgeslagen!";
                    btn.style.backgroundColor = "var(--primary)";
                    alert("Optimalisatie-instellingen succesvol toegepast! De bridge is opnieuw opgestart met de nieuwe parameters.");
                    optimizerSlavesRendered = false;
                } else {
                    alert("Fout bij opslaan: " + result.error);
                    btn.disabled = false;
                    btn.innerText = "✅ Toepassen & Opslaan";
                }
            })
            .catch(err => {
                alert("Netwerkfout bij opslaan: " + err.message);
                btn.disabled = false;
                btn.innerText = "✅ Toepassen & Opslaan";
            });
        }

        let currentScanResults = [];

        function startRegisterScan() {
            const slaveId = parseInt(document.getElementById('scan-slave-id').value);
            const regType = document.getElementById('scan-register-type').value;
            const startAddr = parseInt(document.getElementById('scan-start-addr').value);
            const quantity = parseInt(document.getElementById('scan-quantity').value);
            
            if (isNaN(slaveId) || slaveId < 1 || slaveId > 255) {
                alert("Vul een geldig Slave ID in (1-255).");
                return;
            }
            if (isNaN(startAddr) || startAddr < 0 || startAddr > 65535) {
                alert("Vul een geldig startadres in (0-65535).");
                return;
            }
            if (isNaN(quantity) || quantity < 1 || quantity > 1000) {
                alert("Aantal te scannen registers moet liggen tussen 1 en 1000.");
                return;
            }
            
            const btn = document.getElementById('btn-start-scan');
            btn.disabled = true;
            btn.innerText = "⏳ Scannen...";
            
            const progressContainer = document.getElementById('scan-progress-container');
            const statusText = document.getElementById('scan-status-text');
            const percentText = document.getElementById('scan-percent-text');
            const progressBar = document.getElementById('scan-progress-bar');
            const resultsContainer = document.getElementById('scan-results-container');
            const alertContainer = document.getElementById('scan-alert-container');
            const logConsole = document.getElementById('scan-log-console');
            
            progressContainer.style.display = "block";
            resultsContainer.style.display = "none";
            alertContainer.innerHTML = '';
            logConsole.style.display = "block";
            logConsole.innerHTML = "";
            
            progressBar.style.width = "0%";
            percentText.innerText = "0%";
            statusText.innerText = "Modbus verbinding controleren...";
            
            const scanURL = ingressPath + "/api/scan-registers";
            fetch(scanURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    slave: slaveId,
                    register_type: regType,
                    start_address: startAddr,
                    quantity: quantity
                })
            })
            .then(response => {
                if (!response.ok) throw new Error("Fout status code: " + response.status);
                return response.json();
            })
            .then(result => {
                if (result.success && result.task_id) {
                    pollTaskStatus(result.task_id, 'scanner', slaveId, 0);
                } else {
                    btn.disabled = false;
                    btn.innerText = "⚡ Start Scan";
                    resultsContainer.style.display = "block";
                    alertContainer.innerHTML = `
                        <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                            <strong>❌ Scan opstarten mislukt:</strong><br>${result.error || 'Onbekende fout'}
                        </div>
                    `;
                    document.getElementById('scan-table-body').innerHTML = '<tr><td colspan="4" class="text-center">Scan mislukt.</td></tr>';
                    document.getElementById('scan-select-all').checked = false;
                    document.getElementById('scan-configs-container').style.display = "none";
                }
            })
            .catch(err => {
                btn.disabled = false;
                btn.innerText = "⚡ Start Scan";
                
                progressBar.style.width = "100%";
                progressBar.style.backgroundColor = "var(--danger)";
                statusText.innerText = "Fout opgetreden.";
                statusText.style.color = "var(--danger)";
                
                resultsContainer.style.display = "block";
                alertContainer.innerHTML = `
                    <div class="alert-box alert-box-danger" style="margin-bottom: 0;">
                        <strong>❌ Netwerkfout tijdens scan:</strong><br>${err.message || err}
                    </div>
                `;
                document.getElementById('scan-table-body').innerHTML = '<tr><td colspan="4" class="text-center">Verbindingsfout.</td></tr>';
                document.getElementById('scan-select-all').checked = false;
                document.getElementById('scan-configs-container').style.display = "none";
            });
        }

        function displayScanResults(results) {
            const resultsContainer = document.getElementById('scan-results-container');
            const tbody = document.getElementById('scan-table-body');
            const selectAllBox = document.getElementById('scan-select-all');
            
            resultsContainer.style.display = "block";
            selectAllBox.checked = false;
            document.getElementById('scan-configs-container').style.display = "none";
            
            if (results.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" class="text-center">Geen actieve registers gevonden in deze reeks.</td></tr>';
                return;
            }
            
            let html = '';
            results.forEach(r => {
                const valStr = `int16: <strong>${r.val_int16}</strong> | uint16: <strong>${r.val_uint16}</strong>` +
                    (r.val_int32 !== undefined ? ` | int32: <strong>${r.val_int32}</strong> | uint32: <strong>${r.val_uint32}</strong>` : '');
                    
                html += `
                    <tr>
                        <td style="text-align: center;">
                            <input type="checkbox" class="scan-row-checkbox" data-addr="${r.address}" data-type="${r.register_type}" onclick="handleScanRowSelect()">
                        </td>
                        <td style="font-weight: 600; font-family: monospace;">${r.address}</td>
                        <td><span class="badge" style="background: rgba(255,255,255,0.05); border: 1px solid var(--card-border); color: #fff;">${r.register_type}</span></td>
                        <td style="font-family: monospace; font-size: 0.95rem;">${valStr}</td>
                    </tr>
                `;
            });
            
            tbody.innerHTML = html;
        }

        function toggleSelectAllScan(box) {
            document.querySelectorAll('.scan-row-checkbox').forEach(cb => {
                cb.checked = box.checked;
            });
            handleScanRowSelect();
        }

        function handleScanRowSelect() {
            const listContainer = document.getElementById('scan-config-forms-list');
            const configsContainer = document.getElementById('scan-configs-container');
            
            const checkedRows = Array.from(document.querySelectorAll('.scan-row-checkbox:checked'));
            
            if (checkedRows.length === 0) {
                configsContainer.style.display = "none";
                listContainer.innerHTML = '';
                return;
            }
            
            configsContainer.style.display = "block";
            
            let html = '';
            const slaveId = parseInt(document.getElementById('scan-slave-id').value) || 1;
            
            checkedRows.forEach(row => {
                const addr = parseInt(row.getAttribute('data-addr'));
                const regType = row.getAttribute('data-type');
                
                const item = currentScanResults.find(r => r.address === addr && r.register_type === regType);
                if (!item) return;
                
                const formId = `scan-form-${regType}-${addr}`;
                const nameVal = document.getElementById(`${formId}-name`)?.value || `Slave ${slaveId} Reg ${addr}`;
                const uidVal = document.getElementById(`${formId}-uid`)?.value || `slave_${slaveId}_reg_${addr}`;
                const dataTypeVal = document.getElementById(`${formId}-datatype`)?.value || 'int16';
                const scaleVal = document.getElementById(`${formId}-scale`)?.value || '1.0';
                const precVal = document.getElementById(`${formId}-prec`)?.value || '0';
                const unitVal = document.getElementById(`${formId}-unit`)?.value || '';
                const devClassVal = document.getElementById(`${formId}-devclass`)?.value || '';
                const stateClassVal = document.getElementById(`${formId}-stateclass`)?.value || 'measurement';
                const entTypeVal = document.getElementById(`${formId}-enttype`)?.value || 'sensor';
                const writeVal = document.getElementById(`${formId}-write`)?.checked ? 'checked' : '';
                
                html += `
                    <div class="card" id="${formId}" style="margin-bottom: 1rem; background: rgba(255,255,255,0.01); border-color: rgba(255,255,255,0.04); padding: 1.25rem;">
                        <div style="font-weight: 700; font-size: 1rem; margin-bottom: 0.75rem; color: var(--primary);">
                            🛠️ Register ${addr} (${regType})
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem;">
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Naam</label>
                                <input type="text" id="${formId}-name" value="${nameVal}" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Unique ID</label>
                                <input type="text" id="${formId}-uid" value="${uidVal}" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Data Type</label>
                                <select id="${formId}-datatype" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                                    <option value="int16" ${dataTypeVal === 'int16' ? 'selected' : ''}>int16 (Signed)</option>
                                    <option value="uint16" ${dataTypeVal === 'uint16' ? 'selected' : ''}>uint16 (Unsigned)</option>
                                    ${item.val_int32 !== undefined ? `
                                    <option value="int32" ${dataTypeVal === 'int32' ? 'selected' : ''}>int32 (Signed 32-bit)</option>
                                    <option value="uint32" ${dataTypeVal === 'uint32' ? 'selected' : ''}>uint32 (Unsigned 32-bit)</option>
                                    ` : ''}
                                </select>
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Scale / Factor</label>
                                <input type="number" id="${formId}-scale" step="any" value="${scaleVal}" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Precisie (Decimalen)</label>
                                <input type="number" id="${formId}-prec" min="0" max="10" value="${precVal}" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Eenheid (Unit)</label>
                                <input type="text" id="${formId}-unit" value="${unitVal}" placeholder="bijv. °C, W, V" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Device Class</label>
                                <select id="${formId}-devclass" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                                    <option value="" ${devClassVal === '' ? 'selected' : ''}>geen</option>
                                    <option value="temperature" ${devClassVal === 'temperature' ? 'selected' : ''}>temperature (°C)</option>
                                    <option value="power" ${devClassVal === 'power' ? 'selected' : ''}>power (W)</option>
                                    <option value="energy" ${devClassVal === 'energy' ? 'selected' : ''}>energy (kWh)</option>
                                    <option value="voltage" ${devClassVal === 'voltage' ? 'selected' : ''}>voltage (V)</option>
                                    <option value="current" ${devClassVal === 'current' ? 'selected' : ''}>current (A)</option>
                                    <option value="frequency" ${devClassVal === 'frequency' ? 'selected' : ''}>frequency (Hz)</option>
                                </select>
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">State Class</label>
                                <select id="${formId}-stateclass" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                                    <option value="" ${stateClassVal === '' ? 'selected' : ''}>geen</option>
                                    <option value="measurement" ${stateClassVal === 'measurement' ? 'selected' : ''}>measurement</option>
                                    <option value="total_increasing" ${stateClassVal === 'total_increasing' ? 'selected' : ''}>total_increasing</option>
                                    <option value="total" ${stateClassVal === 'total' ? 'selected' : ''}>total</option>
                                </select>
                            </div>
                            <div>
                                <label style="display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: var(--text-muted);">Entity Type</label>
                                <select id="${formId}-enttype" style="width: 100%; padding: 0.4rem; background: rgba(0,0,0,0.2); border: 1px solid var(--card-border); border-radius: 6px; color: #fff;">
                                    <option value="sensor" ${entTypeVal === 'sensor' ? 'selected' : ''}>sensor</option>
                                    <option value="switch" ${entTypeVal === 'switch' ? 'selected' : ''}>switch</option>
                                    <option value="number" ${entTypeVal === 'number' ? 'selected' : ''}>number</option>
                                </select>
                            </div>
                            <div style="display: flex; align-items: center; gap: 0.5rem; padding-top: 1.25rem;">
                                <input type="checkbox" id="${formId}-write" ${writeVal} style="width: 18px; height: 18px;">
                                <label for="${formId}-write" style="font-size: 0.85rem; font-weight: 600; cursor: pointer; color: var(--text-muted);">Schrijfbaar (Writeable)</label>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            listContainer.innerHTML = html;
        }

        function saveScannedSensors() {
            const checkedRows = Array.from(document.querySelectorAll('.scan-row-checkbox:checked'));
            if (checkedRows.length === 0) {
                alert("Selecteer ten minste één register om toe te voegen.");
                return;
            }
            
            const slaveId = parseInt(document.getElementById('scan-slave-id').value) || 1;
            const sensorsToPost = [];
            
            for (const row of checkedRows) {
                const addr = parseInt(row.getAttribute('data-addr'));
                const regType = row.getAttribute('data-type');
                const formId = `scan-form-${regType}-${addr}`;
                
                const name = document.getElementById(`${formId}-name`).value.trim();
                const uid = document.getElementById(`${formId}-uid`).value.trim();
                const dataType = document.getElementById(`${formId}-datatype`).value;
                const scale = parseFloat(document.getElementById(`${formId}-scale`).value);
                const precision = parseInt(document.getElementById(`${formId}-prec`).value);
                const unit = document.getElementById(`${formId}-unit`).value.trim();
                const devClass = document.getElementById(`${formId}-devclass`).value;
                const stateClass = document.getElementById(`${formId}-stateclass`).value;
                const entityType = document.getElementById(`${formId}-enttype`).value;
                const writeable = document.getElementById(`${formId}-write`).checked;
                
                if (!name) {
                    alert(`Vul a.u.b. een naam in voor register ${addr}.`);
                    return;
                }
                if (!uid) {
                    alert(`Vul a.u.b. een Unique ID in voor register ${addr}.`);
                    return;
                }
                
                sensorsToPost.push({
                    name: name,
                    unique_id: uid,
                    slave: slaveId,
                    register_type: regType,
                    address: addr,
                    data_type: dataType,
                    scale: isNaN(scale) ? 1.0 : scale,
                    precision: isNaN(precision) ? 0 : precision,
                    unit_of_measurement: unit,
                    device_class: devClass,
                    state_class: stateClass,
                    entity_type: entityType,
                    writeable: writeable
                });
            }
            
            const btn = document.getElementById('btn-save-scanned');
            btn.disabled = true;
            btn.innerText = "⏳ Bezig met toevoegen...";
            
            const addURL = ingressPath + "/api/add-sensors";
            fetch(addURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ sensors: sensorsToPost })
            })
            .then(response => response.json().then(data => ({ status: response.status, data })))
            .then(({ status, data }) => {
                btn.disabled = false;
                btn.innerText = "➕ Voeg geselecteerde sensoren toe aan configuratie";
                
                if (status === 200 && data.success) {
                    alert(data.message);
                    switchTab('dashboard');
                    document.getElementById('scan-results-container').style.display = "none";
                    document.getElementById('scan-progress-container').style.display = "none";
                    document.querySelectorAll('.scan-row-checkbox').forEach(cb => cb.checked = false);
                    handleScanRowSelect();
                } else {
                    alert("Fout bij opslaan: " + (data.error || 'Onbekende fout'));
                }
            })
            .catch(err => {
                btn.disabled = false;
                btn.innerText = "➕ Voeg geselecteerde sensoren toe aan configuratie";
                alert("Netwerkfout bij opslaan: " + err.message);
            });
        }

        function updateDashboard() {
            if (activeTab !== 'dashboard') return;
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 8000);
            
            fetch(apiURL, { signal: controller.signal })
                .then(response => {
                    clearTimeout(timeoutId);
                    return response.json();
                })
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
                    const errType = err.name === 'AbortError' ? 'Timeout (>8s)' : err.message || err.name;
                    console.error("Fout bij ophalen status:", errType, "URL:", apiURL);
                    document.getElementById('bridge-status-badge').innerHTML = 
                        `<span class="badge badge-danger" title="${apiURL}: ${errType}">Fout: ${errType}</span>`;
                    document.getElementById('uptime-val').innerText = apiURL;
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
