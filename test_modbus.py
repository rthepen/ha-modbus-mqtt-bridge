import logging
import sys
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType

# Configureer logging voor gedetailleerd inzicht
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("modbus_test")

DEVICES = [
    {"name": "Eth-Dongle-Pro", "host": "192.168.50.213", "port": 502},
    {"name": "USR-N580 Gateway", "host": "192.168.50.96", "port": 41}
]

def test_connection(device_name, host, port, framer_type):
    print("\n" + "="*70)
    print(f"🔄 Testen {device_name} op {host}:{port} met framer: {framer_type.name}")
    print("="*70)
    
    client = ModbusTcpClient(host, port=port, framer=framer_type, timeout=3)
    
    if not client.connect():
        print(f"❌ Kan geen verbinding maken met {host}:{port}")
        return False
        
    print(f"✅ Verbonden met {host}:{port}")
    
    # We proberen Slave 1 (Warmtepomp) Holding Register 0 & 1 & 11 uit te lezen
    print("\n--- Test 1: Warmtepomp (Slave 1) Holding Registers (Adres 0, count=2) ---")
    try:
        res = client.read_holding_registers(address=0, count=2, device_id=1)
        if res.isError():
            print(f"❌ Fout bij uitlezen Slave 1: {res}")
        else:
            print(f"🎉 Succes! Registers AAN/UIT, Modus: {res.registers}")
    except Exception as e:
        print(f"💥 Exception bij Slave 1: {e}")

    print("\n--- Test 1b: Warmtepomp (Slave 1) Holding Register 11 (Doeltemperatuur) ---")
    try:
        res = client.read_holding_registers(address=11, count=1, device_id=1)
        if res.isError():
            print(f"❌ Fout bij uitlezen Slave 1 Reg 11: {res}")
        else:
            print(f"🎉 Succes! Register Doeltemperatuur: {res.registers}")
    except Exception as e:
        print(f"💥 Exception bij Slave 1 Reg 11: {e}")

    # We proberen ook Slave 2 (Sinotimer) Input Register 0 (Spanning) uit te lezen
    print("\n--- Test 2: Sinotimer (Slave 2) Input Registers (Adres 0, count=2) ---")
    try:
        res = client.read_input_registers(address=0, count=2, device_id=2)
        if res.isError():
            print(f"❌ Fout bij uitlezen Slave 2: {res}")
        else:
            print(f"🎉 Succes! Registers: {res.registers}")
            val = res.registers[0]
            print(f"   Spanning meting: {val / 10.0} V" if val > 0 else f"   Spanning meting: {val}")
    except Exception as e:
        print(f"💥 Exception bij Slave 2: {e}")

    # We proberen ook Slave 3 (KWS Meter 1) Holding Register 14 (Spanning) uit te lezen
    print("\n--- Test 3: KWS Meter 1 (Slave 3) Holding Registers (Adres 14, count=1) ---")
    try:
        res = client.read_holding_registers(address=14, count=1, device_id=3)
        if res.isError():
            print(f"❌ Fout bij uitlezen Slave 3: {res}")
        else:
            print(f"🎉 Succes! Registers: {res.registers}")
            val = res.registers[0]
            print(f"   KWS Spanning meting: {val / 100.0} V" if val > 0 else f"   KWS Spanning meting: {val}")
    except Exception as e:
        print(f"💥 Exception bij Slave 3: {e}")
        
    client.close()
    print("🔌 Verbinding gesloten.")

if __name__ == "__main__":
    print("🏁 Starten Modbus Multi-Device scan...")
    
    for dev in DEVICES:
        for ftype in [FramerType.RTU, FramerType.SOCKET]:
            test_connection(dev["name"], dev["host"], dev["port"], ftype)
