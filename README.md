# RThepen Custom Home Assistant Add-ons

This repository contains custom Home Assistant Add-ons designed for high-performance and lightweight command-line execution.

## Add-ons Included:

### 1. **Modbus to MQTT Bridge**
A robust python-based bridge daemon that polls Modbus RTU-over-TCP gateways (like the USR-N580 Gateway) and publishes the readings to Home Assistant via MQTT with full Autodiscovery.

#### Features:
- Native **Home Assistant MQTT Autodiscovery** (creates and updates sensors automatically).
- Lightweight and fast Python implementation running on Alpine.
- Complete support for multiple Modbus slaves and register configurations.
- Custom scale, precision, and signed int16/uint16/int32 register reading logic.

---

## 🚀 How to Add This Repository to Home Assistant

1. Open your Home Assistant frontend.
2. Navigate to **Settings** > **Add-ons** > **Add-on Store**.
3. In the top-right corner, click the **Three Dots menu** (Overflow menu) and select **Repositories**.
4. Paste the URL of this repository:
   ```text
   https://github.com/rthepen/ha-modbus-mqtt-bridge
   ```
5. Click **Add**. Close the popup.
6. The store will refresh, and you will see the **RThepen Custom Home Assistant Add-ons** section with the **Modbus to MQTT Bridge** add-on ready to install!

---

## 🛠️ Configuration Options
Once installed, you can configure your gateway and sensors directly through the Home Assistant UI in the **Configuration** tab. The default options include a complete, pre-configured setup mapping out your Warmtepomp, Sinotimer, and KWS meters. Just change the broker and gateway IPs to match your network, and click Start!
