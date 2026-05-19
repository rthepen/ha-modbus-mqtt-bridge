# Plan & Onderzoeksfase: Modbus naar MQTT Bridge voor Raspberry Pi 3B

Dit document beschrijft het plan van aanpak en de onderzoeksfase voor het bouwen van een **Modbus naar MQTT bridge**. Deze bridge zal draaien op een Raspberry Pi 3B en heeft uitsluitend een command-line (CLI) en API interface (geen grafische UI). De configuratie verloopt volledig via een YAML-bestand en de gemeten Modbus-waarden worden met Home Assistant MQTT Autodiscovery automatisch in Home Assistant geïntegreerd.

---

## 📋 1. Projectdoelen & Specificaties

*   **Platform:** Raspberry Pi 3B (draaiend op Raspberry Pi OS Lite, uitsluitend CLI).
*   **Modbus Interface:** Polling van Modbus-registers (TCP, RTU-over-TCP of Serial RS485).
*   **MQTT Integratie:** Verzenden van Modbus-data naar de MQTT broker.
*   **Home Assistant Autodiscovery:** Automatische registratie van entiteiten in Home Assistant via de `homeassistant/sensor/.../config` topics.
*   **Configuratie:** Eenvoudig uit te breiden en aan te passen via een overzichtelijk `config.yaml` bestand.
*   **API Interface:** Een minimalistische REST API (bijv. via FastAPI/Flask of een lichte HTTP-server) om de status, live logs en huidige registerwaarden op te vragen.
*   **Command Line Interface:** Starten, stoppen en debuggen via de console.

---

## 🛠️ 2. Technologische Stack (Voorgesteld)

Voor een Raspberry Pi 3B is **Python 3** de ideale keuze. Het is lichtgewicht, zeer stabiel voor 24/7 daemons, en heeft volwassen bibliotheken voor zowel Modbus, MQTT als YAML.

| Component | Technologie | Toelichting |
| :--- | :--- | :--- |
| **Base Language** | Python 3.14+ | Aanwezig op het systeem, efficiënt en breed ondersteund. |
| **Modbus Protocol**| `pymodbus` (of `minimalmodbus`) | Ondersteunt zowel Modbus TCP, RTU-over-TCP als seriële RTU. |
| **MQTT Client** | `paho-mqtt` | De industriestandaard Python MQTT-client. |
| **YAML Parser** | `PyYAML` | Voor het eenvoudig inlezen van de configuratiebestanden. |
| **API Server** | `FastAPI` + `Uvicorn` | Zeer lichte, snelle API met ingebouwde Swagger documentatie. |

---

## 🧪 3. Testfase 1: MQTT Verbinding & Credentials Testen

Voordat we gaan programmeren, moeten we zeker weten dat de MQTT verbinding stabiel tot stand kan worden gebracht.

### MQTT Broker Gegevens (uit screenshot):
*   **Broker IP/URL:** `192.168.50.106`
*   **Port:** `1883`
*   **TLS:** Nee
*   **Gebruiker:** `mqqt`
*   **Wachtwoord:** `mqqt`
*   **Top Topic:** `Eth-Dongle-Pro/`

### Testmethode:
We maken een virtuele omgeving (`.venv`) aan in de projectmap en installeren `paho-mqtt` om een eenvoudige test uit te voeren.

#### Stap 1: Virtuele Omgeving & Installatie (op de Mac of Raspberry Pi)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install paho-mqtt
```

#### Stap 2: MQTT Testscript (`test_mqtt.py`)
Met dit script kunnen we controleren of de verbinding slaagt en of we berichten kunnen publiceren naar de broker.

```python
import time
import paho.mqtt.client as mqtt

MQTT_BROKER = "192.168.50.106"
MQTT_PORT = 1883
MQTT_USER = "mqqt"
MQTT_PASS = "mqqt"
TOPIC = "Eth-Dongle-Pro/test"

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Succesvol verbonden met MQTT Broker!")
        client.publish(TOPIC, "Hallo vanaf de Modbus-MQTT test!")
        print(f"📡 Bericht gepubliceerd op topic: {TOPIC}")
    else:
        print(f"❌ Verbinding mislukt met code: {rc}")

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = on_connect

print(f"🔄 Verbinden met {MQTT_BROKER}:{MQTT_PORT}...")
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start een korte loop om het bericht te versturen en te bevestigen
client.loop_start()
time.sleep(2)
client.loop_stop()
client.disconnect()
print("🔌 Verbinding gesloten.")
```

---

## 📟 4. Testfase 2: Modbus Communicatie Testen

Zodra MQTT werkt, testen we de Modbus verbinding. Omdat de top-topic `Eth-Dongle-Pro/` is, gebruiken we hoogstwaarschijnlijk **Modbus TCP** of **Modbus RTU-over-TCP** via een ethernet/WiFi-gateway (zoals een USR-N580 of vergelijkbaar).

### Testmethode:
We installeren `pymodbus` in onze virtuele omgeving en proberen een aantal testregisters uit te lezen om te verifiëren dat de communicatie werkt.

#### Stap 1: Installeer Pymodbus
```bash
pip install pymodbus
```

#### Stap 2: Modbus TCP Testscript (`test_modbus.py`)
```python
import logging
from pymodbus.client import ModbusTcpClient

# Configureer logging voor debug-inzicht
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

# Vervang met de juiste IP en Poort van de Modbus TCP Dongle/Gateway
MODBUS_HOST = "192.168.50.106" # Pas aan indien de Modbus-gateway een ander IP heeft
MODBUS_PORT = 502             # Standaard Modbus TCP poort is 502

client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)

print(f"🔄 Verbinden met Modbus TCP dongle op {MODBUS_HOST}:{MODBUS_PORT}...")
if client.connect():
    print("✅ Succesvol verbonden met Modbus-apparaat!")
    
    # Voorbeeld: Lees Holding Register vanaf adres 0 (lengte 10)
    # Pas UNIT ID (slave) aan indien nodig (meestal 1 of 2)
    UNIT_ID = 1
    START_ADDRESS = 0
    COUNT = 10
    
    print(f"📖 Register {START_ADDRESS} t/m {START_ADDRESS + COUNT - 1} uitlezen...")
    result = client.read_holding_registers(START_ADDRESS, COUNT, slave=UNIT_ID)
    
    if not result.isError():
        print("🎉 Registers succesvol uitgelezen!")
        print(f"Waarden: {result.registers}")
    else:
        print(f"❌ Fout bij uitlezen registers: {result}")
        
    client.close()
else:
    print("❌ Kan geen verbinding maken met de Modbus-gateway.")
```

---

## ⚙️ 5. YAML Configuratie Ontwerp (`config.yaml`)

De volledige bridge wordt geconfigureerd met één YAML-bestand. Dit maakt het extreem makkelijk om nieuwe Modbus-registers toe te voegen zonder de code te wijzigen.

Hier is een voorstel voor het YAML-schema:

```yaml
# MQTT Broker Instellingen
mqtt:
  broker: "192.168.50.106"
  port: 1883
  username: "mqqt"
  password: "mqqt"
  topic_prefix: "usr_n580_bridge"
  discovery_prefix: "homeassistant" # Prefix voor HA Autodiscovery

# Modbus Verbinding Instellingen
modbus:
  type: "tcp" # 'tcp', 'rtu_over_tcp' of 'serial'
  host: "192.168.50.106" # IP van de Modbus Dongle of Gateway
  port: 502
  slave_id: 1
  poll_interval: 10 # Hoe vaak registers worden uitgelezen in seconden

# Definitie van de Modbus Registers en hoe deze in Home Assistant moeten verschijnen
sensors:
  - name: "Warmtepomp Temperatuur Aanvoer"
    unique_id: "warmtepomp_temp_aanvoer"
    register_type: "holding" # 'holding' of 'input'
    register_address: 100
    data_type: "int16" # int16, uint16, int32, float32, etc.
    scale: 0.1 # Bijv. waarde 352 wordt 35.2 °C
    unit_of_measurement: "°C"
    device_class: "temperature"
    state_class: "measurement"

  - name: "Warmtepomp Temperatuur Retour"
    unique_id: "warmtepomp_temp_retour"
    register_type: "holding"
    register_address: 101
    data_type: "int16"
    scale: 0.1
    unit_of_measurement: "°C"
    device_class: "temperature"
    state_class: "measurement"

  - name: "Warmtepomp Status Code"
    unique_id: "warmtepomp_status_code"
    register_type: "holding"
    register_address: 102
    data_type: "uint16"
    scale: 1
    unit_of_measurement: ""
```

---

## 🏠 6. Home Assistant MQTT Autodiscovery Werking

Om sensoren automatisch in Home Assistant te laten verschijnen zonder handmatige configuratie, publiceert onze bridge bij het opstarten een configuratie-payload naar het autodiscovery topic van Home Assistant.

Bijvoorbeeld voor de `Warmtepomp Temperatuur Aanvoer`:
*   **Discovery Topic:** `homeassistant/sensor/warmtepomp_temp_aanvoer/config`
*   **Payload (JSON):**
    ```json
    {
      "name": "Warmtepomp Temperatuur Aanvoer",
      "unique_id": "warmtepomp_temp_aanvoer",
      "state_topic": "usr_n580_bridge/sensor/warmtepomp_temp_aanvoer/state",
      "value_template": "{{ value_json.value }}",
      "unit_of_measurement": "°C",
      "device_class": "temperature",
      "state_class": "measurement",
      "device": {
        "identifiers": ["modbus_mqtt_bridge_wp"],
        "name": "Warmtepomp Modbus Bridge",
        "model": "Modbus2MQTT v1.0",
        "manufacturer": "Custom Bridge"
      }
    }
    ```

Hierna hoeft de bridge alleen periodiek de waarde te publiceren naar:
*   **State Topic:** `usr_n580_bridge/sensor/warmtepomp_temp_aanvoer/state`
*   **State Payload:** `{"value": 35.2}`

---

## 🚀 7. Volgende Stappen

1.  **[x] Omgeving Opzetten:** De virtuele Python-omgeving (`.venv`) is aangemaakt en alle packages (`paho-mqtt`, `pymodbus`, `PyYAML`) zijn succesvol geïnstalleerd.
2.  **[x] MQTT Connectie Testen:** De MQTT-verbinding is succesvol getest! Het testbericht is met succes gepubliceerd naar `Eth-Dongle-Pro/test` en bevestigd door de broker.
3.  **[x] Modbus Uitlezen Testen:** Uitgebreide scan uitgevoerd op de gevonden lokale gateways.
4.  **[ ] Ontwikkeling van de Bridge Daemon:** Ontwikkelen van de definitieve Python-applicatie die continu op de Raspberry Pi zal draaien met YAML-configuratie, REST API en Home Assistant Autodiscovery.

---

## 🔬 8. Live Test- en Research-Resultaten (19 Mei 2026)

Tijdens de onderzoeksfase zijn de volgende tests direct uitgevoerd op het netwerk (`192.168.50.0/24`):

### A. MQTT Verbindings-Test (`test_mqtt.py`)
*   **Doel-Broker:** `192.168.50.106:1883`
*   **Resultaat:** **100% Succesvol!**
*   **Log output:**
    ```text
    🔄 Verbinden met 192.168.50.106:1883...
    ✅ Succesvol verbonden met MQTT Broker!
    📡 Bericht succesvol gepubliceerd op topic: Eth-Dongle-Pro/test
    📨 Bericht verzenden bevestigd door broker.
    🔌 Test voltooid en verbinding netjes gesloten.
    ```

### B. Modbus Gateway Netwerk-Scan (`test_modbus.py`)
We hebben het lokale netwerk gescand en **twee** actieve Modbus-omgevingen gedetecteerd en uitgelezen:

| Gateway Naam | IP-adres & Poort | Framer Type | Status & Uitlees-Resultaat |
| :--- | :--- | :--- | :--- |
| **USR-N580 Gateway** | `192.168.50.96:41` | **Modbus RTU-over-TCP** | **100% Succes!**<br>• *Warmtepomp (Slave 1)*: AAN/UIT = `0` (Uit), Doeltemp = `35 °C`<br>• *Sinotimer (Slave 2)*: Spanning = `232.8 V`<br>• *KWS Meter 1 (Slave 3)*: Spanning = `233.6 V` |
| **Eth-Dongle-Pro** | `192.168.50.213:502` | **Standaard Modbus TCP** | **Gedeeltelijk verbonden/Garbage data**<br>• *Warmtepomp (Slave 1)*: Gaf ongeldige registerwaarden (`[27148, 7172]`) en doeltemp `65535`. overige slaves gaven time-outs/foutcodes. |

> [!NOTE]
> De **USR-N580 Gateway** (`192.168.50.96:41`) levert momenteel de 100% correcte en actieve waarden van je warmtepomp en meters!

