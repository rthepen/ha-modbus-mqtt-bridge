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
                    "sensors": ha_options.get("sensors", [])
                }
                logger.info("Addon opties succesvol ingeladen en getransformeerd!")
                return
            except Exception as e:
                logger.error(f"Fout bij inlezen Home Assistant opties: {e}")
                sys.exit(1)

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

            # Bepaal apparaatnaam en identifiers op basis van slave ID
            slave = s.get("slave", 1)
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

            # Autodiscovery payload conform Home Assistant standaarden
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

        if not self.mqtt_connected:
            mqtt_logger.warning(f"MQTT niet verbonden. Waarde voor {unique_id} overgeslagen.")
            return

        # Zoek het entiteitstype voor het juiste topic
        entity_type = "sensor"
        for s in self.config.get("sensors", []):
            if s.get("unique_id") == unique_id:
                entity_type = s.get("entity_type", "sensor")
                break

        topic_prefix = self.config.get("mqtt", {}).get("topic_prefix", "usr_n580_bridge")
        state_topic = f"{topic_prefix}/{entity_type}/{unique_id}/state"
        payload = json.dumps({"value": value})

        try:
            mqtt_logger.debug(f"Publiceer: {state_topic} -> {payload}")
            self.mqtt_client.publish(state_topic, payload)
        except Exception as e:
            mqtt_logger.error(f"Fout bij publiceren status voor {unique_id}: {e}")

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
                self.publish_sensor_value(uid, float(payload) if entity_type != "switch" else (1 if val == 1 else 0))
                return
                
            # Alleen holding registers kunnen geschreven worden in Modbus
            reg_type = sensor.get("register_type", "holding")
            if reg_type != "holding":
                modbus_logger.error(f"Fout: Alleen holding registers zijn schrijfbaar (sensor {uid} heeft type {reg_type})")
                return
                
            res = self.modbus_client.write_register(address=address, value=val, device_id=slave)
            if res.isError():
                modbus_logger.error(f"Fout bij schrijven naar Modbus voor {uid}: {res}")
            else:
                modbus_logger.info(f"Succesvol geschreven naar Modbus voor {uid}: Raw {val}")
                # Direct de nieuwe status terug publiceren naar MQTT zodat de UI meteen geüpdatet wordt!
                time.sleep(0.2)
                actual_val = self.read_sensor(sensor)
                if actual_val is not None:
                    self.publish_sensor_value(uid, actual_val)
                    
        except Exception as e:
            modbus_logger.error(f"Exception bij schrijven naar Modbus voor {uid}: {e}")


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
        
        # Belangrijk: USR-N580 gebruikt RTU framing over TCP
        self.modbus_client = ModbusTcpClient(
            host, 
            port=port, 
            framer=FramerType.RTU, 
            timeout=timeout
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
            if reg_type == "holding":
                res = self.modbus_client.read_holding_registers(address=address, count=1, device_id=slave)
            elif reg_type == "input":
                res = self.modbus_client.read_input_registers(address=address, count=1, device_id=slave)
            else:
                modbus_logger.error(f"Onbekend register type: {reg_type} voor {uid}")
                return None

            if res.isError():
                modbus_logger.error(f"Fout bij uitlezen {uid} (Slave {slave}, Adres {address}): {res}")
                return None

            # Interpreteer de waarde (meestal een signed int16)
            raw_value = res.registers[0]
            
            # Verwerk signed integers handmatig indien int16
            if sensor.get("data_type", "int16") == "int16" and raw_value >= 32768:
                raw_value -= 65536

            # Toepassen van schaling en precisie
            calculated_value = round(raw_value * scale, precision) if precision > 0 else int(round(raw_value * scale))
            
            modbus_logger.debug(f"Gelezen {uid}: Raw={res.registers[0]} -> Berekend={calculated_value}")
            return calculated_value

        except Exception as e:
            modbus_logger.error(f"Exception bij uitlezen {uid}: {e}")
            return None

    # --------------------------------------------------------------------------
    # Hoofd Polling Loop
    # --------------------------------------------------------------------------
    def poll_all_sensors(self):
        """Lees alle geconfigureerde sensoren en publiceer hun waarden."""
        sensors = self.config.get("sensors", [])
        logger.info(f"Starten pollingronde voor {len(sensors)} sensoren...")
        
        failed_slaves = set()
        success_count = 0
        
        for s in sensors:
            slave = s.get("slave", 1)
            uid = s.get("unique_id")
            
            # Sla over als deze slave al gemarkeerd is als mislukt in deze pollingronde
            if slave in failed_slaves:
                modbus_logger.debug(f"Sla {uid} over omdat Slave {slave} in deze ronde al gefaald is.")
                continue
                
            val = self.read_sensor(s)
            
            if val is not None:
                self.publish_sensor_value(uid, val)
                success_count += 1
            else:
                # Markeer als mislukt zodat we overige registers voor deze slave overslaan in deze ronde
                failed_slaves.add(slave)
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

    def terminate(self):
        """Zet de vlag om de daemon te stoppen."""
        self.running = False


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
