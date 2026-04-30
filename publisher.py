"""
publisher.py - Renewable Energy Sensor Publishers
Group Project - Distributed Systems (Option 2)

Two publisher modes:
    solar  - Simulates a rooftop solar PV node (solar irradiance, temperature, energy output)
    wind   - Simulates a small wind turbine node (wind speed, direction, energy output)

Each publisher connects to the broker and periodically sends sensor readings
as PUBLISH messages on the appropriate topics.

Topics published by solar mode:
    solar/output        - full solar sensor reading
    weather/irradiance  - irradiance + weather features (used by ANN engine)
    alerts/low_output   - fired if output drops unexpectedly during daylight hours

Topics published by wind mode:
    wind/output         - full wind turbine reading
    weather/wind_speed  - wind speed + direction features (used by ANN engine)
    alerts/low_output   - fired if wind speed is dangerously high (turbine cutoff)

References:
    Coulouris et al., Distributed Systems, 5th Ed., Chapter 4 (IPC/sockets)
    Chapter 6.3 - Indirect communication, publisher role
"""

import socket
import json
import time
import random
import argparse
from datetime import datetime


HOST = '127.0.0.1'
PORT = 5555


def send_msg(sock, msg_dict):
    """Encode and send a single JSON message with newline terminator."""
    payload = json.dumps(msg_dict) + '\n'
    sock.sendall(payload.encode('utf-8'))



# Solar sensor simulation


def solar_reading(hour, month=None):
    """
    Simulate a solar PV node reading for a given hour of the day.

    Irradiance follows a rough bell curve peaking at solar noon (hour 12).
    Temperature correlates with irradiance. Energy output is a function
    of irradiance, panel efficiency, and temperature derating.

    Args:
        hour  (int): 0-23, current hour
        month (int): 1-12, used for seasonal adjustment (defaults to current month)
    Returns:
        dict: sensor reading
    """
    import math

    if month is None:
        month = datetime.now().month

    # Seasonal factor - solar resource peaks around June in northern hemisphere
    # (adjust sign for southern hemisphere if needed)
    season_factor = 1.0 + 0.25 * math.sin(2 * math.pi * (month - 3) / 12)

    # Irradiance bell curve - zero outside daylight hours
    if 6 <= hour <= 19:
        peak = 900 * season_factor
        irradiance = peak * math.exp(-0.5 * ((hour - 12) / 3.5) ** 2)
        irradiance += random.gauss(0, 55)
        irradiance = max(0.0, irradiance)
    else:
        irradiance = 0.0

    temperature = 18 + (irradiance / 100) * 0.28 + random.gauss(0, 1.5)
    humidity = max(20, min(100, 75 - temperature * 0.4 + random.gauss(0, 7)))

    # Simple PV energy model
    panel_area = 25        # m²
    efficiency = 0.175     # 17.5% standard module efficiency
    temp_coeff = 0.004     # power loss per °C above 25°C

    gross_solar = (irradiance * efficiency * panel_area) / 1000.0  # kWh
    temp_derating = 1.0 - temp_coeff * max(0, temperature - 25)
    energy_kwh = max(0.0, gross_solar * temp_derating * random.uniform(0.93, 1.03))

    return {
        'source': 'solar_node_01',
        'irradiance': round(irradiance, 2),
        'temperature': round(temperature, 2),
        'humidity': round(humidity, 2),
        'hour': hour,
        'energy_kwh': round(energy_kwh, 4)
    }


def run_solar_publisher(interval=3.0):
    """
    Connect to the broker and publish solar readings on a fixed interval.
    Publishes to solar/output, weather/irradiance, and alerts/low_output as needed.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print(f"[Solar Publisher] Connected to broker at {HOST}:{PORT}")
    print(f"[Solar Publisher] Publishing every {interval}s\n")

    try:
        while True:
            now = datetime.now()
            reading = solar_reading(now.hour, now.month)
            ts = now.isoformat()

            # Topic 1: full solar output 
            send_msg(sock, {
                'type': 'PUBLISH',
                'topic': 'solar/output',
                'data': reading,
                'timestamp': ts
            })
            print(f"[Solar] solar/output  | {reading['energy_kwh']} kWh  "
                  f"| irr={reading['irradiance']} W/m²  | temp={reading['temperature']}°C")

            # Topic 2: weather features only (for ANN engine subscriber) 
            weather_payload = {
                'source': reading['source'],
                'irradiance': reading['irradiance'],
                'temperature': reading['temperature'],
                'humidity': reading['humidity'],
                'hour': reading['hour']
            }
            send_msg(sock, {
                'type': 'PUBLISH',
                'topic': 'weather/irradiance',
                'data': weather_payload,
                'timestamp': ts
            })

            # Topic 3: low output alert 
            # During daylight (8-17h), output below 0.4 kWh is unexpected
            if reading['energy_kwh'] < 0.4 and 8 <= now.hour <= 17:
                send_msg(sock, {
                    'type': 'PUBLISH',
                    'topic': 'alerts/low_output',
                    'data': {
                        'source': 'solar_node_01',
                        'reason': 'low_solar_output',
                        'value': reading['energy_kwh'],
                        'threshold': 0.4,
                        'hour': reading['hour']
                    },
                    'timestamp': ts
                })
                print(f"[Solar] alerts/low_output SENT (output={reading['energy_kwh']} kWh at hour {now.hour})")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[Solar Publisher] Stopped by user.")
    except BrokenPipeError:
        print("[Solar Publisher] Lost connection to broker.")
    finally:
        try:
            send_msg(sock, {'type': 'DISCONNECT'})
        except Exception:
            pass
        sock.close()



# Wind sensor simulation


def wind_reading():
    """
    Simulate a wind turbine node reading.

    Uses a simple power curve model:
        below cut-in speed (3 m/s) -> no generation
        between cut-in and rated (12 m/s) -> cubic ramp-up
        above rated -> full output
        above cut-out (20 m/s) -> turbine shuts down for safety

    Returns:
        dict: sensor reading
    """
    wind_speed = max(0.0, random.gauss(7.5, 3.0))   # mean ~7.5 m/s
    direction = random.uniform(0.0, 360.0)

    rated_power_kw = 2.0   # 2 kW turbine
    cut_in = 3.0
    rated = 12.0
    cut_out = 20.0

    if wind_speed < cut_in or wind_speed > cut_out:
        capacity_factor = 0.0
    elif wind_speed >= rated:
        capacity_factor = 1.0
    else:
        capacity_factor = ((wind_speed - cut_in) / (rated - cut_in)) ** 3

    energy_kwh = rated_power_kw * capacity_factor * random.uniform(0.95, 1.05)

    return {
        'source': 'wind_node_01',
        'wind_speed': round(wind_speed, 2),
        'direction': round(direction, 1),
        'capacity_factor': round(capacity_factor, 4),
        'hour': datetime.now().hour,
        'energy_kwh': round(energy_kwh, 4)
    }


def run_wind_publisher(interval=4.0):
    """
    Connect to the broker and publish wind turbine readings on a fixed interval.
    Publishes to wind/output, weather/wind_speed, and alerts/low_output as needed.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print(f"[Wind Publisher] Connected to broker at {HOST}:{PORT}")
    print(f"[Wind Publisher] Publishing every {interval}s\n")

    try:
        while True:
            reading = wind_reading()
            ts = datetime.now().isoformat()

            # Topic 1: full wind output 
            send_msg(sock, {
                'type': 'PUBLISH',
                'topic': 'wind/output',
                'data': reading,
                'timestamp': ts
            })
            print(f"[Wind]  wind/output   | {reading['energy_kwh']} kWh  "
                  f"| speed={reading['wind_speed']} m/s  | cf={reading['capacity_factor']}")

            # Topic 2: weather wind speed features (for ANN engine) 
            send_msg(sock, {
                'type': 'PUBLISH',
                'topic': 'weather/wind_speed',
                'data': {
                    'source': reading['source'],
                    'wind_speed': reading['wind_speed'],
                    'direction': reading['direction'],
                    'hour': reading['hour']
                },
                'timestamp': ts
            })

            # Topic 3: dangerously high wind (turbine cut-out zone) 
            if reading['wind_speed'] > 18.0:
                send_msg(sock, {
                    'type': 'PUBLISH',
                    'topic': 'alerts/low_output',
                    'data': {
                        'source': 'wind_node_01',
                        'reason': 'high_wind_cutout',
                        'value': reading['wind_speed'],
                        'threshold': 18.0
                    },
                    'timestamp': ts
                })
                print(f"[Wind]  alerts/low_output SENT (cutout wind speed: {reading['wind_speed']} m/s)")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[Wind Publisher] Stopped by user.")
    except BrokenPipeError:
        print("[Wind Publisher] Lost connection to broker.")
    finally:
        try:
            send_msg(sock, {'type': 'DISCONNECT'})
        except Exception:
            pass
        sock.close()



# Entry point


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Renewable Energy Publisher - sends sensor data to broker'
    )
    parser.add_argument(
        '--mode', choices=['solar', 'wind'], required=True,
        help='solar = solar PV node, wind = wind turbine node'
    )
    parser.add_argument(
        '--interval', type=float, default=3.0,
        help='Seconds between publications (default: 3.0)'
    )
    args = parser.parse_args()

    if args.mode == 'solar':
        run_solar_publisher(args.interval)
    else:
        run_wind_publisher(args.interval)
