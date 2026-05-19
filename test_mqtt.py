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
        # Zodra verbonden, publiceren we een testbericht
        result = client.publish(TOPIC, "Hallo vanaf de Modbus-MQTT test!")
        status = result[0]
        if status == 0:
            print(f"📡 Bericht succesvol gepubliceerd op topic: {TOPIC}")
        else:
            print(f"❌ Fout bij publiceren van bericht: {status}")
    else:
        print(f"❌ Verbinding mislukt met code: {rc}")

def on_publish(client, userdata, mid, reason_code=None, properties=None):
    print("📨 Bericht verzenden bevestigd door broker.")

# Gebruik callback API versie 2 (vereist in paho-mqtt 2.x)
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USER, MQTT_PASS)

client.on_connect = on_connect
client.on_publish = on_publish

print(f"🔄 Verbinden met {MQTT_BROKER}:{MQTT_PORT}...")
try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    # Start loop en wacht even om de berichten af te handelen
    client.loop_start()
    time.sleep(3)
    client.loop_stop()
    client.disconnect()
    print("🔌 Test voltooid en verbinding netjes gesloten.")
except Exception as e:
    print(f"❌ Uitzondering opgetreden: {e}")
