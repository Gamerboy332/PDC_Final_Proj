# Renewable Energy Pub-Sub Messaging System
**Distributed Systems - Group Project (Option 2)**

A broker-based publish-subscribe middleware for real-time renewable energy
monitoring, featuring:
- Topic-based message routing
- At-least-once delivery via persistent queues
- ANN-powered prediction engine as a subscriber

---

## Project Structure

```
├── broker.py               Central message broker (run first)
├── publisher.py            Solar and wind sensor publishers
├── subscriber.py           Grid Monitor, ANN Engine, Alert Service
├── ann_model.py            Neural network model (Negnevitsky Ch. 6)
├── dataset_generator.py    Generates synthetic training data
├── requirements.txt        Python dependencies
└── README.md               This file
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```
> The core pub-sub system (broker, publisher, subscriber) uses only the Python
> standard library. The ANN model and dataset generator require the packages
> listed in requirements.txt.

### 2. (Optional) Prepare the ANN model
If you want the ANN subscriber to make live predictions, train it first:
```bash
# Step 1: Generate training data (creates renewable_energy_dataset.csv)
python dataset_generator.py

# Step 2: Train the neural network (creates ann_model_weights.keras + ann_scalers.pkl)
python ann_model.py
```

---

## Running the System

Each component runs in its own terminal window. Open **6 terminals** (or 4 minimum).

### Terminal 1 — Start the Broker
```bash
python broker.py
```
The broker must be running before any publishers or subscribers connect.

---

### Terminal 2 — Solar Publisher
```bash
python publisher.py --mode solar --interval 3
```
Publishes to topics: `solar/output`, `weather/irradiance`, `alerts/low_output`

### Terminal 3 — Wind Publisher
```bash
python publisher.py --mode wind --interval 4
```
Publishes to topics: `wind/output`, `weather/wind_speed`, `alerts/low_output`

---

### Terminal 4 — Grid Monitor Subscriber
```bash
python subscriber.py --mode grid
```
Subscribes to: `solar/output`, `wind/output`, `alerts/low_output`
Tracks running totals of energy generated each session.

### Terminal 5 — ANN Prediction Engine
```bash
python subscriber.py --mode ann
```
Subscribes to: `weather/irradiance`, `weather/wind_speed`
Runs the trained neural network to predict energy output for incoming weather readings.

### Terminal 6 — Alert Service
```bash
python subscriber.py --mode alert
```
Subscribes to: `alerts/low_output` only.
Simulates email/SMS notifications when alerts are received.

---

## Demo: Demonstrating Key Features

### Feature 1 — Topic Filtering
Run all three subscribers and both publishers. You will see:
- Grid Monitor receives `solar/output` and `wind/output` — the ANN Engine does NOT.
- Alert Service only receives messages when an alert fires.
- ANN Engine only receives weather feature messages, not full energy readings.

### Feature 2 — Persistent Queue / At-Least-Once Delivery
1. Start the broker, both publishers, and the Alert Service.
2. **Stop the Alert Service** (`Ctrl+C`).
3. Wait 30-60 seconds while publishers continue sending (including some alerts).
4. **Restart the Alert Service**: `python subscriber.py --mode alert`
5. Watch the Alert Service immediately receive all alerts it missed while offline.

This works because the broker persists undelivered messages to `broker_queues.pkl`
on disk, so they survive even a broker restart.

### Feature 3 — Reconnection
Stop any subscriber and restart it. It automatically reconnects and picks up
any missed messages — no manual intervention needed.

---

## Architecture

```
  Solar Node (publisher.py --mode solar)
       |
       |  PUBLISH solar/output
       |  PUBLISH weather/irradiance
       |  PUBLISH alerts/low_output
       |
       v
  +--------------------+          solar/output       --> Grid Monitor
  |       BROKER       |  ------> wind/output         --> Grid Monitor
  |     broker.py      |  ------> alerts/low_output   --> Grid Monitor
  |                    |  ------> alerts/low_output   --> Alert Service
  | - subscriptions    |  ------> weather/irradiance  --> ANN Engine
  | - persistent       |  ------> weather/wind_speed  --> ANN Engine
  |   queues per       |
  |   client           |
  +--------------------+
       ^
       |  PUBLISH wind/output
       |  PUBLISH weather/wind_speed
       |  PUBLISH alerts/low_output
       |
  Wind Node (publisher.py --mode wind)
```

### Message Protocol (JSON over TCP, newline-delimited)

| Type       | Direction              | Fields                                     |
|------------|------------------------|--------------------------------------------|
| SUBSCRIBE  | client → broker        | client_id, topics[]                        |
| PUBLISH    | publisher → broker     | topic, data{}, timestamp                   |
| MESSAGE    | broker → subscriber    | msg_id, topic, data{}, timestamp           |
| ACK        | subscriber → broker    | msg_id                                     |
| DISCONNECT | client → broker        | (empty body)                               |

---

## Neural Network Architecture (ann_model.py)

Based on Negnevitsky (2005), Chapter 6:

```
Input Layer      Hidden Layer 1    Hidden Layer 2    Output Layer
(5 neurons)      (10 neurons)      (8 neurons)       (1 neuron)

irradiance   \                                      /
temperature   |   Dense(10, ReLU)  Dense(8, ReLU)  |   energy_kwh
humidity      +-->                               -->+-->  (kWh)
wind_speed    |   §6.4 Fig 6.8     §6.4 Fig 6.8   |
hour         /                                      \
```

Training uses backpropagation (Negnevitsky §6.4, Steps 1–4) with the Adam
optimizer, which implements momentum and adaptive learning rates (§6.5, Eq. 6.17).

---

## Files Generated at Runtime

| File                          | Purpose                                    |
|-------------------------------|--------------------------------------------|
| `broker_queues.pkl`           | Persisted message queues (auto-created)    |
| `renewable_energy_dataset.csv`| Training data (created by dataset_generator.py) |
| `ann_model_weights.keras`     | Saved ANN weights (created by ann_model.py)|
| `ann_scalers.pkl`             | Saved feature scalers (created by ann_model.py) |

---

## References

- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011).
  *Distributed Systems: Concepts and Design* (5th ed.). Pearson.
  Chapters 4, 5, 6, 7.

- Negnevitsky, M. (2005).
  *Artificial Intelligence: A Guide to Intelligent Systems* (2nd ed.).
  Pearson Education Limited.
  Chapter 6: Artificial Neural Networks (pp. 184–232).

- Glorot, X., & Bengio, Y. (2010). Understanding the difficulty of training
  deep feedforward neural networks. AISTATS.
  *(Justification for ReLU over sigmoid in hidden layers)*

- Kingma, D. P., & Ba, J. (2014). Adam: A method for stochastic optimization.
  arXiv:1412.6980. *(Justification for Adam optimizer)*

---

*Borrowed code: None. All code written by the group.*
*Dataset source: Synthetically generated using physics-based simulation.*
