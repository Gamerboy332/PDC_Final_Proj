"""
subscriber.py - Renewable Energy Pub-Sub Subscribers
Group Project - Distributed Systems (Option 2)

Three subscriber modes:
    grid    - Grid Monitor: subscribes to all energy output topics, tracks totals
    ann     - ANN Prediction Engine: subscribes to weather feature topics,
              runs the trained neural network to forecast energy output
    alert   - Alert Service: subscribes only to alert topics, simulates notifications

All three modes reconnect automatically if the broker goes down, and receive
queued messages upon reconnection (at-least-once delivery, Ch. 6.3).

References:
    Coulouris et al., Distributed Systems, 5th Ed.
    Chapter 6.3 - Indirect communication, subscriber role and durable subscriptions
    Chapter 4   - Interprocess communication (TCP sockets)
"""

import socket
import json
import time
import argparse
from datetime import datetime


HOST = '127.0.0.1'
PORT = 5555
RECONNECT_DELAY = 5   # seconds to wait before retrying a lost connection



# Socket helpers

def send_msg(sock, msg_dict):
    """Encode and send a JSON message with newline terminator."""
    payload = json.dumps(msg_dict) + '\n'
    sock.sendall(payload.encode('utf-8'))


def listen_for_messages(sock, on_message):
    """
    Blocking loop that reads newline-delimited JSON messages from sock
    and calls on_message(msg_dict, sock) for each complete message received.

    Passes sock into the callback so each subscriber mode can send an ACK
    immediately after processing, satisfying at-least-once delivery
    (Coulouris Ch. 6.3).

    Uses a buffer to handle TCP fragmentation - a single recv() call
    may return a partial message or multiple messages at once.
    """
    buf = ''
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            print("[Subscriber] Broker closed the connection.")
            break

        buf += chunk.decode('utf-8')

        while '\n' in buf:
            line, buf = buf.split('\n', 1)
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                on_message(msg, sock)
            except json.JSONDecodeError:
                print(f"[Subscriber] Could not parse message: {line[:60]}")


def send_ack(sock, msg_id):
    """
    Send an ACK back to the broker for a received message.

    This completes the at-least-once delivery handshake defined in
    Coulouris Ch. 6.3: the broker keeps the message in its unacked table
    until it receives this confirmation. If the ACK never arrives the
    broker will retry delivery up to MAX_RETRIES times.

    Args:
        sock   (socket): the active connection to the broker
        msg_id (str):    the msg_id field from the MESSAGE received
    """
    try:
        send_msg(sock, {'type': 'ACK', 'msg_id': msg_id})
    except OSError:
        pass  # connection already gone — broker watchdog will handle the retry



# Core connection + subscription loop


def connect_and_run(client_id, topics, on_message):
    """
    Connect to the broker, send a SUBSCRIBE message, and listen for messages.
    Retries automatically after any connection failure.

    The automatic reconnect is what enables at-least-once delivery:
    when the subscriber reconnects, the broker drains its queue of
    messages that arrived while it was offline.

    Args:
        client_id  (str):  unique name for this subscriber instance
        topics     (list): list of topic strings to subscribe to
        on_message (func): callback(msg_dict) called for each MESSAGE received
    """
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            print(f"[{client_id}] Connected to broker at {HOST}:{PORT}")

            # Register subscriptions with the broker
            send_msg(sock, {
                'type': 'SUBSCRIBE',
                'client_id': client_id,
                'topics': topics
            })
            print(f"[{client_id}] Subscribed to topics: {topics}")
            print(f"[{client_id}] Waiting for messages...\n")

            # listen_for_messages blocks until the connection drops
            listen_for_messages(sock, on_message)

        except ConnectionRefusedError:
            print(f"[{client_id}] Cannot reach broker. Is broker.py running? "
                  f"Retrying in {RECONNECT_DELAY}s...")
        except OSError as e:
            print(f"[{client_id}] Connection error: {e}. Retrying in {RECONNECT_DELAY}s...")
        except Exception as e:
            print(f"[{client_id}] Unexpected error: {e}. Retrying in {RECONNECT_DELAY}s...")
        finally:
            try:
                sock.close()
            except Exception:
                pass

        # Wait before trying to reconnect
        print(f"[{client_id}] Reconnecting in {RECONNECT_DELAY}s...")
        time.sleep(RECONNECT_DELAY)



# Subscriber Mode 1: Grid Monitor


def run_grid_monitor():
    """
    Subscribes to all energy output topics.
    Accumulates running totals of solar and wind generation and logs everything.

    Topics:
        solar/output       - solar PV readings
        wind/output        - wind turbine readings
        alerts/low_output  - any system alerts
    """
    client_id = 'grid_monitor'
    topics = ['solar/output', 'wind/output', 'alerts/low_output']

    # Running totals
    totals = {'solar': 0.0, 'wind': 0.0, 'messages': 0}

    def on_message(msg, sock):
        if msg.get('type') != 'MESSAGE':
            return

        topic = msg.get('topic', '')
        data = msg.get('data', {})
        ts = msg.get('timestamp', 'N/A')
        msg_id = msg.get('msg_id', '')
        totals['messages'] += 1

        print(f"\n[Grid Monitor] ===== Message #{totals['messages']} =====")
        print(f"  Topic     : {topic}")
        print(f"  Timestamp : {ts}")

        if topic == 'solar/output':
            kwh = data.get('energy_kwh', 0.0)
            totals['solar'] += kwh
            print(f"  Solar output   : {kwh} kWh")
            print(f"  Irradiance     : {data.get('irradiance')} W/m²")
            print(f"  Temperature    : {data.get('temperature')}°C")
            print(f"  Session total  : {round(totals['solar'], 4)} kWh (solar)")

        elif topic == 'wind/output':
            kwh = data.get('energy_kwh', 0.0)
            totals['wind'] += kwh
            print(f"  Wind output    : {kwh} kWh")
            print(f"  Wind speed     : {data.get('wind_speed')} m/s")
            print(f"  Cap. factor    : {data.get('capacity_factor')}")
            print(f"  Session total  : {round(totals['wind'], 4)} kWh (wind)")

        elif topic == 'alerts/low_output':
            print(f"  *** ALERT ***")
            print(f"  Source  : {data.get('source')}")
            print(f"  Reason  : {data.get('reason', 'N/A')}")
            print(f"  Value   : {data.get('value')}  |  Threshold: {data.get('threshold', 'N/A')}")

        combined = round(totals['solar'] + totals['wind'], 4)
        print(f"  Combined total : {combined} kWh")

        # ACK the broker so it removes this message from the unacked table.
        # Without this the broker would retry delivery after ACK_TIMEOUT seconds.
        send_ack(sock, msg_id)

    connect_and_run(client_id, topics, on_message)


# Subscriber Mode 2: ANN Prediction Engine


def run_ann_engine():
    """
    Subscribes to weather feature topics and uses the trained ANN model
    to predict energy output for incoming sensor readings.

    Topics:
        weather/irradiance   - solar weather features
        weather/wind_speed   - wind weather features

    If no trained model is found, the subscriber still runs and just
    prints the raw features it receives.
    """
    client_id = 'ann_engine'
    topics = ['weather/irradiance', 'weather/wind_speed']

    # Try to load the trained model at startup
    ann = None
    try:
        import numpy as np
        from ann_model import RenewableEnergyANN
        candidate = RenewableEnergyANN()
        if candidate.load_model():
            ann = candidate
            print(f"[ANN Engine] Trained model loaded successfully.")
        else:
            print(f"[ANN Engine] No saved model found.")
            print(f"[ANN Engine] Run:  python ann_model.py  to train first.")
            print(f"[ANN Engine] Continuing in feature-display mode.\n")
    except ImportError as e:
        print(f"[ANN Engine] Import error: {e}")
        print(f"[ANN Engine] Continuing without prediction capability.\n")

    def on_message(msg, sock):
        if msg.get('type') != 'MESSAGE':
            return

        topic = msg.get('topic', '')
        data = msg.get('data', {})
        ts = msg.get('timestamp', 'N/A')
        msg_id = msg.get('msg_id', '')

        print(f"\n[ANN Engine] Received on '{topic}' @ {ts}")

        if ann is not None:
            try:
                import numpy as np

                if topic == 'weather/irradiance':
                    features = np.array([[
                        data.get('irradiance', 0.0),
                        data.get('temperature', 25.0),
                        data.get('humidity', 60.0),
                        0.0,
                        data.get('hour', datetime.now().hour)
                    ]])
                else:  # weather/wind_speed
                    features = np.array([[
                        0.0,
                        25.0,
                        60.0,
                        data.get('wind_speed', 0.0),
                        data.get('hour', datetime.now().hour)
                    ]])

                prediction = ann.predict(features)
                pred_val = round(float(prediction[0][0]), 4)
                print(f"  [ANN Engine] Predicted energy output: {pred_val} kWh")
                print(f"  [ANN Engine] Features used: {features[0].tolist()}")

            except Exception as e:
                print(f"  [ANN Engine] Prediction failed: {e}")
        else:
            print(f"  Features received: {data}")

        send_ack(sock, msg_id)

    connect_and_run(client_id, topics, on_message)


# Subscriber Mode 3: Alert Service


def run_alert_service():
    """
    Subscribes only to alert topics.
    Logs all alerts and simulates sending notifications (email/SMS).

    In a real deployment this would integrate with an SMTP server or SMS gateway.
    The important thing for the demo is that:
        1. It receives alerts in real time when connected.
        2. When it reconnects after being offline, it receives any alerts it missed.

    Topics:
        alerts/low_output
    """
    client_id = 'alert_service'
    topics = ['alerts/low_output']

    alert_log = []

    def on_message(msg, sock):
        if msg.get('type') != 'MESSAGE':
            return

        data = msg.get('data', {})
        ts = msg.get('timestamp', 'N/A')
        msg_id = msg.get('msg_id', '')

        alert_entry = {
            'received_at': datetime.now().isoformat(),
            'event_time': ts,
            'source': data.get('source', 'unknown'),
            'reason': data.get('reason', 'N/A'),
            'value': data.get('value', 'N/A'),
            'threshold': data.get('threshold', 'N/A')
        }
        alert_log.append(alert_entry)

        print(f"\n[Alert Service] !!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"  ALERT #{len(alert_log)}")
        print(f"  Event time : {ts}")
        print(f"  Received   : {alert_entry['received_at']}")
        print(f"  Source     : {alert_entry['source']}")
        print(f"  Reason     : {alert_entry['reason']}")
        print(f"  Value      : {alert_entry['value']}  |  Threshold: {alert_entry['threshold']}")
        print(f"  [Simulated] Email sent to: grid_ops@renewablesite.local")
        print(f"  [Simulated] SMS sent to:   +63 9XX XXX XXXX")
        print(f"  Total alerts this session: {len(alert_log)}")
        print(f"[Alert Service] !!!!!!!!!!!!!!!!!!!!!!!!!!!")

        send_ack(sock, msg_id)

    connect_and_run(client_id, topics, on_message)



# Entry point


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Renewable Energy Subscriber - connects to broker and receives messages'
    )
    parser.add_argument(
        '--mode', choices=['grid', 'ann', 'alert'], required=True,
        help=(
            'grid  = Grid Monitor (all energy output topics)\n'
            'ann   = ANN Prediction Engine (weather feature topics)\n'
            'alert = Alert Service (alert topics only)'
        )
    )
    args = parser.parse_args()

    if args.mode == 'grid':
        run_grid_monitor()
    elif args.mode == 'ann':
        run_ann_engine()
    elif args.mode == 'alert':
        run_alert_service()
