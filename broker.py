"""
broker.py - Central Message Broker for the Publish-Subscribe System
Group Project - Distributed Systems (Option 2)

The broker handles all message routing between publishers and subscribers.
It keeps track of who is subscribed to what topic and queues messages
for subscribers that are offline (persistent/durable queues).

References:
    Coulouris et al., Distributed Systems: Concepts and Design, 5th Ed.
    Chapter 6.3 - Publish-Subscribe (indirect communication)
    Chapter 4   - Interprocess Communication (socket-based messaging)
"""

import socket
import threading
import json
import os
import pickle
import time
import uuid
from collections import deque
from datetime import datetime


# Configuration
HOST = '127.0.0.1'
PORT = 5555
BUFFER_SIZE = 4096
QUEUE_FILE = 'broker_queues.pkl'  # persists queues across broker restarts

#  Shared state (protected by a lock) 
# topic -> list of client_ids that want messages on that topic
subscriptions = {}

# client_id -> deque of undelivered messages (at-least-once delivery)
queues = {}

# client_id -> active socket connection
active_connections = {}

# msg_id -> { 'client_id', 'msg', 'sent_at', 'retries' }
# Tracks messages sent but not yet ACK'd — enables at-least-once retry
unacked = {}

# How long to wait before retrying an unacknowledged message (seconds)
ACK_TIMEOUT = 10.0

# Maximum number of retries before giving up and re-queuing for next reconnect
MAX_RETRIES = 3

state_lock = threading.Lock()



# Queue persistence helpers


def load_queues():
    """
    Load queues from disk so messages survive a broker restart.
    If the file doesn't exist yet that's fine - just start with empty queues.
    """
    global queues
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, 'rb') as f:
                queues = pickle.load(f)
            total = sum(len(q) for q in queues.values())
            print(f"[Broker] Loaded persistent queues: {len(queues)} client(s), {total} pending message(s)")
        except Exception as e:
            print(f"[Broker] Warning - could not load queues from disk: {e}")
            queues = {}
    else:
        print("[Broker] No existing queue file found, starting fresh.")


def save_queues():
    """Write the current queue state to disk."""
    try:
        with open(QUEUE_FILE, 'wb') as f:
            pickle.dump(queues, f)
    except Exception as e:
        print(f"[Broker] Warning - could not save queues: {e}")



# ACK retry watchdog


def ack_watchdog():
    """
    Background thread that runs forever, waking every ACK_TIMEOUT seconds
    to check for messages that were delivered but never acknowledged.

    At-least-once delivery guarantee (Coulouris Ch. 6.3):
        A message is considered delivered only when the subscriber sends
        an ACK back. If no ACK arrives within ACK_TIMEOUT seconds the
        broker re-sends the message up to MAX_RETRIES times. After that
        the message goes back into the persistent queue so it will be
        delivered when the subscriber reconnects.
    """
    while True:
        time.sleep(ACK_TIMEOUT)
        now = time.monotonic()

        with state_lock:
            timed_out = [
                (mid, meta) for mid, meta in unacked.items()
                if now - meta['sent_at'] >= ACK_TIMEOUT
            ]

        for msg_id, meta in timed_out:
            client_id = meta['client_id']
            retries   = meta['retries']
            msg       = meta['msg']

            with state_lock:
                sock = active_connections.get(client_id)

            if sock is not None and retries < MAX_RETRIES:
                # Subscriber is still connected — re-send
                success = send_json(sock, msg)
                if success:
                    with state_lock:
                        if msg_id in unacked:
                            unacked[msg_id]['retries'] += 1
                            unacked[msg_id]['sent_at'] = time.monotonic()
                    print(f"[Broker] ACK timeout: resent msg_id={msg_id} "
                          f"to '{client_id}' (retry {retries + 1}/{MAX_RETRIES})")
                else:
                    # Socket broke during retry — move to queue
                    with state_lock:
                        unacked.pop(msg_id, None)
                        active_connections.pop(client_id, None)
                        if client_id not in queues:
                            queues[client_id] = deque()
                        queues[client_id].append(msg)
                        save_queues()
                    print(f"[Broker] Retry send failed for msg_id={msg_id}, "
                          f"re-queued for '{client_id}'")
            else:
                # Max retries exhausted or subscriber offline — push back to queue
                with state_lock:
                    unacked.pop(msg_id, None)
                    if client_id not in queues:
                        queues[client_id] = deque()
                    queues[client_id].append(msg)
                    save_queues()
                print(f"[Broker] msg_id={msg_id} max retries reached or "
                      f"'{client_id}' offline — re-queued for next reconnect")



# Socket helpers


def send_json(sock, msg_dict):
    """
    Serialise msg_dict to JSON and send it over sock.
    Messages are newline-terminated so the receiver knows where each one ends.
    Returns True on success, False if the send failed.
    """
    try:
        payload = json.dumps(msg_dict) + '\n'
        sock.sendall(payload.encode('utf-8'))
        return True
    except (BrokenPipeError, ConnectionResetError, OSError):
        return False



# Per-client connection handler


def handle_client(conn, addr):
    """
    Runs in its own thread for each connected client.
    Reads newline-delimited JSON messages and handles them based on type.

    Message types we expect from clients:
        SUBSCRIBE  - client registers topics it cares about
        PUBLISH    - publisher sends data for a topic
        ACK        - subscriber confirms receipt (at-least-once tracking)
        DISCONNECT - graceful shutdown from client side
    """
    client_id = None
    recv_buffer = ''

    print(f"[Broker] New connection from {addr}")

    try:
        while True:
            chunk = conn.recv(BUFFER_SIZE)
            if not chunk:
                break  # client closed the connection

            recv_buffer += chunk.decode('utf-8')

            # A single recv() might contain multiple messages or a partial message.
            # We split on newline and keep any incomplete tail in the buffer.
            while '\n' in recv_buffer:
                raw_msg, recv_buffer = recv_buffer.split('\n', 1)
                raw_msg = raw_msg.strip()
                if not raw_msg:
                    continue

                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    print(f"[Broker] Bad JSON from {addr}: {raw_msg[:80]}")
                    continue

                msg_type = msg.get('type', '')

                
                # SUBSCRIBE - client tells us which topics it wants
               
                if msg_type == 'SUBSCRIBE':
                    client_id = msg.get('client_id')
                    requested_topics = msg.get('topics', [])

                    if not client_id:
                        print(f"[Broker] SUBSCRIBE message missing client_id from {addr}")
                        continue

                    with state_lock:
                        # Register this socket as the active connection
                        active_connections[client_id] = conn

                        # Create a queue for this client if we haven't seen it before
                        if client_id not in queues:
                            queues[client_id] = deque()

                        # Add the client to each requested topic's subscriber list
                        for topic in requested_topics:
                            if topic not in subscriptions:
                                subscriptions[topic] = []
                            if client_id not in subscriptions[topic]:
                                subscriptions[topic].append(client_id)

                        print(f"[Broker] '{client_id}' subscribed to: {requested_topics}")

                        # Pull out any queued messages so we can send them after releasing the lock
                        pending = list(queues[client_id])
                        queues[client_id].clear()
                        save_queues()

                    # Deliver any messages that were queued while this client was offline.
                    # This is the at-least-once delivery mechanism described in Ch. 6.3.
                    if pending:
                        print(f"[Broker] Delivering {len(pending)} queued message(s) to '{client_id}'")
                        for queued_msg in pending:
                            if not send_json(conn, queued_msg):
                                # Failed to send - put it back in the queue
                                with state_lock:
                                    queues[client_id].appendleft(queued_msg)
                                    save_queues()
                                break

                
                # PUBLISH - publisher sends a message for a topic
                
                elif msg_type == 'PUBLISH':
                    topic = msg.get('topic')
                    if not topic:
                        continue

                    # Wrap the message with a unique ID for tracking
                    msg_id = str(uuid.uuid4())[:8]
                    routed = {
                        'type': 'MESSAGE',
                        'msg_id': msg_id,
                        'topic': topic,
                        'data': msg.get('data', {}),
                        'timestamp': msg.get('timestamp', datetime.now().isoformat())
                    }

                    with state_lock:
                        targets = list(subscriptions.get(topic, []))

                    delivered = 0
                    queued_count = 0

                    for target_id in targets:
                        with state_lock:
                            target_sock = active_connections.get(target_id)

                        if target_sock is not None:
                            success = send_json(target_sock, routed)
                            if success:
                                # Register this message as pending ACK
                                with state_lock:
                                    unacked[msg_id + '_' + target_id] = {
                                        'client_id': target_id,
                                        'msg':       routed,
                                        'sent_at':   time.monotonic(),
                                        'retries':   0
                                    }
                                delivered += 1
                            else:
                                # Socket broke - remove it and queue the message
                                with state_lock:
                                    active_connections.pop(target_id, None)
                                    if target_id not in queues:
                                        queues[target_id] = deque()
                                    queues[target_id].append(routed)
                                    save_queues()
                                queued_count += 1
                        else:
                            # Subscriber is offline - queue the message for later
                            with state_lock:
                                if target_id not in queues:
                                    queues[target_id] = deque()
                                queues[target_id].append(routed)
                                save_queues()
                            queued_count += 1

                    if targets:
                        print(f"[Broker] Topic '{topic}' -> {delivered} delivered, {queued_count} queued")
                    # If no subscribers at all, the message is just dropped (fire-and-forget)

                
                # ACK - subscriber confirms it received a message
                
                elif msg_type == 'ACK':
                    msg_id = msg.get('msg_id')
                    if not msg_id:
                        continue

                    with state_lock:
                        if msg_id in unacked:
                            unacked.pop(msg_id)
                            # print(f"[Broker] ACK confirmed: msg_id={msg_id} from '{client_id}'")

                
                # DISCONNECT - clean shutdown from client
                
                elif msg_type == 'DISCONNECT':
                    print(f"[Broker] '{client_id}' sent DISCONNECT")
                    break

    except ConnectionResetError:
        pass
    except Exception as e:
        print(f"[Broker] Unexpected error handling {addr}: {e}")
    finally:
        # Clean up the active connection entry, but keep the queue and subscriptions
        # so the client can reconnect later and pick up missed messages.
        if client_id:
            with state_lock:
                active_connections.pop(client_id, None)
            print(f"[Broker] '{client_id}' disconnected from {addr}")
        else:
            print(f"[Broker] Unidentified client from {addr} disconnected")
        conn.close()



# Main broker startup


def start_broker():
    load_queues()

    # Start the ACK retry watchdog as a background daemon thread
    watchdog = threading.Thread(target=ack_watchdog, daemon=True, name='ack-watchdog')
    watchdog.start()
    print(f"[Broker] ACK watchdog started (timeout={ACK_TIMEOUT}s, max_retries={MAX_RETRIES})")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow reuse so we can restart quickly without waiting for TIME_WAIT
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(20)

    print(f"[Broker] Renewable Energy Pub-Sub Broker started on {HOST}:{PORT}")
    print(f"[Broker] Waiting for publishers and subscribers...\n")

    try:
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[Broker] Shutting down.")
    finally:
        server_sock.close()


if __name__ == '__main__':
    start_broker()
