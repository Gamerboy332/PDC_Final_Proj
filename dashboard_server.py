"""
dashboard_server.py - Real-Time Dashboard Bridge Server
Group Project - Distributed Systems (Option 2)

This server acts as a special subscriber to the pub-sub broker.
It receives all topic messages and re-broadcasts them to any open
browser via Server-Sent Events (SSE), which the dashboard.html
page listens to.

Architecture:
    broker.py  <--TCP-->  dashboard_server.py  <--SSE-->  browser (dashboard.html)

Run order:
    1. python broker.py
    2. python publisher.py --mode solar
    3. python publisher.py --mode wind
    4. python subscriber.py --mode grid   (optional alongside dashboard)
    5. python dashboard_server.py         <- this file
    Then open http://localhost:5050 in your browser
"""

import socket
import threading
import json
import time
import queue
from datetime import datetime
from flask import Flask, Response, render_template_string, jsonify
import os

# Broker connection config 
BROKER_HOST = '127.0.0.1'
BROKER_PORT = 5555
CLIENT_ID   = 'dashboard'
TOPICS      = [
    'solar/output',
    'wind/output',
    'weather/irradiance',
    'weather/wind_speed',
    'alerts/low_output',
    'system/status'        # client connect/disconnect events from broker
]
RECONNECT_DELAY = 5

# Flask app 
app = Flask(__name__)

# Shared state 
# Latest reading per topic - served to new browser connections immediately
latest_state = {
    'solar':       None,
    'wind':        None,
    'irradiance':  None,
    'wind_speed':  None,
    'alerts':      [],
    'messages':    [],
    'connected':   False,
    'last_update': None
}

state_lock   = threading.Lock()

# SSE listener registry - each open browser tab gets its own queue
sse_clients  = []
sse_lock     = threading.Lock()


# SSE broadcast helpers

def broadcast(event_type, data_dict):
    """Push an SSE event to all connected browser tabs."""
    payload = json.dumps(data_dict)
    msg = f"event: {event_type}\ndata: {payload}\n\n"
    dead = []
    with sse_lock:
        for q in sse_clients:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            sse_clients.remove(q)


def send_broker_msg(sock, msg_dict):
    payload = json.dumps(msg_dict) + '\n'
    sock.sendall(payload.encode('utf-8'))


def send_ack(sock, msg_id):
    """Send ACK back to broker confirming message receipt (at-least-once delivery)."""
    try:
        send_broker_msg(sock, {'type': 'ACK', 'msg_id': msg_id})
    except OSError:
        pass


# Human-readable labels for each client_id
CLIENT_LABELS = {
    'solar_publisher': 'Solar Publisher  (Terminal 2)',
    'wind_publisher':  'Wind Publisher   (Terminal 3)',
    'grid_monitor':    'Grid Monitor     (Terminal 4)',
    'ann_engine':      'ANN Engine       (Terminal 5)',
    'alert_service':   'Alert Service    (Terminal 6)',
    'dashboard':       'Dashboard Server (Terminal 7)',
}


def _add_system_alert(client_id, reason, detail=''):
    """
    Build a system-level alert entry and broadcast it to the UI.
    These are distinct from energy alerts — they report node connectivity.
    """
    now_str = datetime.now().strftime('%H:%M:%S')
    label   = CLIENT_LABELS.get(client_id, client_id)

    alert = {
        'type':      'system',          # tells the UI to render differently
        'source':    label,
        'reason':    reason,
        'value':     detail,
        'threshold': '',
        'time':      now_str
    }

    with state_lock:
        latest_state['alerts'].insert(0, alert)
        latest_state['alerts'] = latest_state['alerts'][:20]

    broadcast('system_alert', alert)
    print(f"[Dashboard] SYSTEM ALERT: {label} — {reason}")


# Broker subscriber thread


def broker_listener():
    """
    Runs forever in a background thread.
    Connects to the broker, subscribes to all topics, and processes
    incoming messages, updating latest_state and broadcasting to browsers.
    """
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((BROKER_HOST, BROKER_PORT))

            send_broker_msg(sock, {
                'type': 'SUBSCRIBE',
                'client_id': CLIENT_ID,
                'topics': TOPICS
            })

            with state_lock:
                latest_state['connected'] = True

            broadcast('status', {'connected': True, 'message': 'Connected to broker'})
            print(f"[Dashboard] Connected to broker, subscribed to {len(TOPICS)} topics")

            buf = ''
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break

                buf += chunk.decode('utf-8')
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if msg.get('type') != 'MESSAGE':
                        continue

                    process_message(msg, sock)

        except ConnectionRefusedError:
            with state_lock:
                latest_state['connected'] = False
            # Broker went completely offline — show as system alert on UI
            _add_system_alert('broker', 'broker_offline',
                              'Broker (Terminal 1) is offline or unreachable')
            broadcast('status', {'connected': False, 'message': 'Broker offline — retrying...'})
            print(f"[Dashboard] Broker not reachable. Retrying in {RECONNECT_DELAY}s...")
        except Exception as e:
            with state_lock:
                latest_state['connected'] = False
            _add_system_alert('broker', 'broker_connection_lost', str(e))
            broadcast('status', {'connected': False, 'message': f'Connection lost: {e}'})
            print(f"[Dashboard] Connection error: {e}")
        finally:
            try:
                sock.close()
            except Exception:
                pass

        time.sleep(RECONNECT_DELAY)


def process_message(msg, sock):
    """Handle a MESSAGE from the broker and update state + broadcast."""
    topic   = msg.get('topic', '')
    data    = msg.get('data', {})
    ts      = msg.get('timestamp', datetime.now().isoformat())
    msg_id  = msg.get('msg_id', '')

    send_ack(sock, msg_id)

    now_str = datetime.now().strftime('%H:%M:%S')

    log_entry = {
        'time':  now_str,
        'topic': topic,
        'data':  data
    }

    with state_lock:
        latest_state['last_update'] = now_str
        latest_state['messages'].insert(0, log_entry)
        latest_state['messages'] = latest_state['messages'][:50]

        if topic == 'solar/output':
            latest_state['solar'] = {**data, 'time': now_str}

        elif topic == 'wind/output':
            latest_state['wind'] = {**data, 'time': now_str}

        elif topic == 'weather/irradiance':
            latest_state['irradiance'] = {**data, 'time': now_str}

        elif topic == 'weather/wind_speed':
            latest_state['wind_speed'] = {**data, 'time': now_str}

        elif topic == 'alerts/low_output':
            alert = {**data, 'type': 'energy', 'time': now_str}
            latest_state['alerts'].insert(0, alert)
            latest_state['alerts'] = latest_state['alerts'][:20]

    # Broadcast outside the lock
    if topic == 'solar/output':
        broadcast('solar', {**data, 'time': now_str})
    elif topic == 'wind/output':
        broadcast('wind', {**data, 'time': now_str})
    elif topic == 'weather/irradiance':
        broadcast('irradiance', {**data, 'time': now_str})
    elif topic == 'weather/wind_speed':
        broadcast('wind_speed', {**data, 'time': now_str})
    elif topic == 'alerts/low_output':
        broadcast('alert', {**data, 'type': 'energy', 'time': now_str})
    elif topic == 'system/status':
        event     = data.get('event', '')
        client_id = data.get('client_id', 'unknown')
        if event == 'disconnected':
            _add_system_alert(
                client_id,
                'client_disconnected',
                f"{CLIENT_LABELS.get(client_id, client_id)} went offline"
            )

    broadcast('log', log_entry)


# Flask routes


@app.route('/')
def index():
    """Serve the dashboard HTML file."""
    html_path = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        return f.read()


@app.route('/api/state')
def api_state():
    """Return the current snapshot of all latest readings as JSON."""
    with state_lock:
        return jsonify(latest_state)


@app.route('/stream')
def stream():
    """
    Server-Sent Events endpoint.
    Each browser tab that connects gets its own queue.
    Messages are pushed as they arrive from the broker.
    """
    def event_stream(q):
        # Send current state immediately on connect
        with state_lock:
            snap = json.dumps(dict(latest_state))
        yield f"event: snapshot\ndata: {snap}\n\n"

        while True:
            try:
                msg = q.get(timeout=25)
                yield msg
            except Exception:
                # Heartbeat to keep connection alive
                yield ": heartbeat\n\n"

    q = queue.Queue(maxsize=100)
    with sse_lock:
        sse_clients.append(q)

    return Response(
        event_stream(q),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


# Startup


if __name__ == '__main__':
    # Start the broker listener in a background thread
    t = threading.Thread(target=broker_listener, daemon=True, name='broker-listener')
    t.start()

    print("[Dashboard] Server starting at http://localhost:5050")
    print("[Dashboard] Open that URL in your browser to see the live dashboard.")
    print("[Dashboard] Make sure broker.py and publishers are already running.\n")

    app.run(host='0.0.0.0', port=5050, debug=False, threaded=True)