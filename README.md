# Renewable Energy Pub-Sub Messaging System
**Distributed Systems — Group Project (Option 2)**
**Due: May 7, 2026 | Location: Iloilo City, Philippines**

A broker-based publish-subscribe middleware for real-time renewable energy
monitoring built on raw Python TCP sockets, featuring:

- Topic-based message routing (Coulouris et al., Ch. 6.3)
- At-least-once delivery via persistent durable queues + ACK retry watchdog
- Live browser dashboard with weather-style UI
- ANN-powered prediction engine as a subscriber (Negnevitsky, Ch. 6)
- System disconnect alerts when any terminal goes offline

---

## Project Structure

```
├── broker.py               Central message broker — run first
├── publisher.py            Solar PV and wind turbine sensor publishers
├── subscriber.py           Grid Monitor, ANN Engine, Alert Service
├── ann_model.py            Neural network model (Negnevitsky Ch. 6)
├── dataset_generator.py    Synthetic training data generator
├── dashboard_server.py     Flask SSE bridge server for the browser UI
├── dashboard.html          Live weather-style browser dashboard
├── renewable_energy_dataset.csv  Training dataset (auto-generated)
├── requirements.txt        Python dependencies
└── README.md               This file
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

The core pub-sub system (`broker.py`, `publisher.py`, `subscriber.py`) uses
only the Python standard library — no extra packages needed for those.
The ANN model, dataset generator, and dashboard server require the packages
in `requirements.txt` (TensorFlow, Flask, NumPy, pandas, scikit-learn).

### 2. Prepare the ANN model (one-time, do before running the system)
```bash
# Step 1 — Generate training data
python dataset_generator.py

# Step 2 — Train the neural network
python ann_model.py
```

Wait for each step to fully complete before running the next.
This creates `renewable_energy_dataset.csv`, `ann_model_weights.keras`,
and `ann_scalers.pkl`. These only need to be generated once.

---

## Running the System

Open **7 terminal windows** in VS Code (`Ctrl+Shift+`` ` ```, split with `+`).
Start them **in order** — the broker must be running before anything else.

### Terminal 1 — Broker (start first, always)
```bash
python broker.py
```
Starts the central TCP message broker on `127.0.0.1:5555`.
Loads any persisted queues from `broker_queues.pkl` on startup.

### Terminal 2 — Solar Publisher
```bash
python publisher.py --mode solar --interval 3
```
Publishes to topics: `solar/output`, `weather/irradiance`
Simulates a rooftop solar PV node with irradiance, temperature, and humidity
readings. Output is 0.0 kWh at night (18:00-06:00) — physically accurate.

### Terminal 3 — Wind Publisher
```bash
python publisher.py --mode wind --interval 4
```
Publishes to topics: `wind/output`, `weather/wind_speed`
Simulates a 2 kW wind turbine node with speed, direction, and capacity factor.

### Terminal 4 — Grid Monitor Subscriber
```bash
python subscriber.py --mode grid
```
Subscribes to: `solar/output`, `wind/output`, `alerts/low_output`
Accumulates running session totals for solar and wind generation.

### Terminal 5 — ANN Prediction Engine
```bash
python subscriber.py --mode ann
```
Subscribes to: `weather/irradiance`, `weather/wind_speed`
Loads the trained neural network and predicts energy output for each
incoming weather feature message. Falls back to feature-display mode
if no trained model is found — run `python ann_model.py` first.

### Terminal 6 — Alert Service
```bash
python subscriber.py --mode alert
```
Subscribes to: `alerts/low_output`
Logs alerts and simulates email/SMS notifications.

### Terminal 7 — Live Dashboard Server
```bash
python dashboard_server.py
```
Then open **http://localhost:5050** in your browser.

The dashboard subscribes to all topics and streams live data to the browser
via Server-Sent Events (SSE). Shows all publishers and subscribers in one UI.

---

## Terminal Layout (VS Code)

```
+------------------+-------------------+
|  broker.py       |  publisher solar   |
|  Terminal 1      |  Terminal 2        |
+------------------+-------------------+
|  subscriber      |  publisher wind    |
|  --mode grid     |  Terminal 3        |
|  Terminal 4      |                    |
+------------------+-------------------+
|  subscriber      |  subscriber        |
|  --mode ann      |  --mode alert      |
|  Terminal 5      |  Terminal 6        |
+------------------+-------------------+
|  dashboard_server.py                  |
|  Terminal 7  ->  localhost:5050       |
+---------------------------------------+
```

---

## Demonstrating the 3 Required Features

### Feature 1 — Topic Filtering
With all 7 terminals running, observe each terminal:
- Terminal 4 (Grid Monitor) receives `solar/output` and `wind/output` — Terminal 5 does NOT
- Terminal 5 (ANN Engine) receives only `weather/irradiance` and `weather/wind_speed`
- Terminal 6 (Alert Service) receives nothing unless an alert fires
- The dashboard live log shows each topic colour-coded and routed to the correct subscriber only

### Feature 2 — Persistent Queue / At-Least-Once Delivery
1. Start all 7 terminals
2. Stop Terminal 6 (Alert Service) with `Ctrl+C`
3. Stop Terminal 1 (Broker) and restart it — queues survive via `broker_queues.pkl`
4. Wait 30-60 seconds while publishers keep sending
5. Restart Terminal 6: `python subscriber.py --mode alert`
6. The Alert Service immediately receives all messages it missed while offline
7. The dashboard shows: Alert Service (Terminal 6) went offline — then reconnected

### Feature 3 — Disconnect System Alerts
Stop any terminal with `Ctrl+C`. Within seconds the dashboard's
SYSTEM ALERTS panel shows which terminal went offline with a timestamp.
Each alert card has a RESOLVE X button to dismiss it once the issue is fixed.

---

## Architecture

```
  Solar Node (publisher.py --mode solar)
       |
       |  PUBLISH solar/output
       |  PUBLISH weather/irradiance
       |
       v
  +------------------------+
  |        BROKER          |  solar/output       ---> Grid Monitor (T4)
  |       broker.py        |  wind/output        ---> Grid Monitor (T4)
  |                        |  alerts/low_output  ---> Grid Monitor (T4)
  |  subscriptions dict    |  alerts/low_output  ---> Alert Service (T6)
  |  persistent queues     |  weather/irradiance ---> ANN Engine   (T5)
  |  ACK watchdog          |  weather/wind_speed ---> ANN Engine   (T5)
  |  disconnect notify     |  all topics         ---> Dashboard    (T7)
  |  system/status topic   |  system/status      ---> Dashboard    (T7)
  +------------------------+
       ^
       |  PUBLISH wind/output
       |  PUBLISH weather/wind_speed
       |
  Wind Node (publisher.py --mode wind)


  Dashboard Server (dashboard_server.py)
       |  subscribes to all topics via TCP
       |  re-broadcasts via Server-Sent Events (SSE)
       v
  Browser (dashboard.html) at http://localhost:5050
```

---

## Message Protocol (JSON over TCP, newline-delimited)

| Type         | Direction            | Fields                                        |
|--------------|----------------------|-----------------------------------------------|
| `SUBSCRIBE`  | client -> broker     | `client_id`, `topics[]`                       |
| `PUBLISH`    | publisher -> broker  | `topic`, `data{}`, `timestamp`                |
| `MESSAGE`    | broker -> subscriber | `msg_id`, `topic`, `data{}`, `timestamp`      |
| `ACK`        | subscriber -> broker | `msg_id`                                      |
| `DISCONNECT` | client -> broker     | (empty body)                                  |

### At-Least-Once Delivery Flow
```
Publisher  ->  PUBLISH  ->  Broker
                              |
                   registers msg in unacked{}
                              |
                   MESSAGE  ->  Subscriber
                              |
                   waits ACK_TIMEOUT = 10 seconds
                              |
             if no ACK: retry up to MAX_RETRIES = 3
             if offline: persist to broker_queues.pkl
                              |
                   ACK  <--  Subscriber
                              |
                   removes from unacked{}
```

---

## Dashboard UI (http://localhost:5050)

| Panel | What it Shows |
|---|---|
| Solar — PV Node 01 | Live kWh, irradiance bar, temperature, humidity. Sun dims at night |
| Wind — Turbine Node 01 | Live kWh, compass needle rotating to actual direction, turbine spin |
| Grid — Session Totals | Combined kWh since session start, per-source bars, message count |
| System Alerts | Node disconnect events — each has RESOLVE X button to dismiss |
| ANN Predictions | Status of the ANN Engine subscriber processing weather features |
| Live Message Log | All messages across all topics, colour-coded by type, with CLEAR button |

---

## Neural Network Architecture

Based on Negnevitsky (2005), Chapter 6, Sections 6.1-6.5:

```
Input Layer      Hidden Layer 1    Hidden Layer 2    Output Layer
(5 neurons)      (10 neurons)      (8 neurons)       (1 neuron)

irradiance   \                                       /
temperature   |   Dense(10, ReLU)  Dense(8, ReLU)   |   energy_kwh
humidity      +-->                               --> +-->   (kWh)
wind_speed    |   Sec 6.4 Fig 6.8  Sec 6.4 Fig 6.8  |
hour         /                                       \
```

| Parameter | Value | Textbook Reference |
|---|---|---|
| Optimizer | Adam, lr=0.001 | Negnevitsky Sec 6.5 Eq. 6.17 (momentum) |
| Loss function | MSE | Negnevitsky Sec 6.4 Step 4 |
| Weight initialisation | Glorot uniform | Negnevitsky Sec 6.4 Step 1 |
| Early stopping | patience=20 | Negnevitsky Sec 6.4 Step 4 (convergence) |
| Batch size | 32 | Mini-batch gradient descent |
| Train/test split | 80/20 chronological | No shuffle — avoids data leakage |
| Activation (hidden) | ReLU | Glorot & Bengio (2010) — avoids vanishing gradient |
| Activation (output) | Linear | Regression output — unbounded kWh values |

---

## Files Generated at Runtime

| File | Created By | Purpose |
|---|---|---|
| `broker_queues.pkl` | `broker.py` | Persistent message queues — survives restarts |
| `renewable_energy_dataset.csv` | `dataset_generator.py` | Synthetic training data (8,760 rows) |
| `ann_model_weights.keras` | `ann_model.py` | Saved trained ANN weights |
| `ann_scalers.pkl` | `ann_model.py` | Saved MinMaxScaler parameters for inference |

---

## References

- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011).
  *Distributed Systems: Concepts and Design* (5th ed.). Pearson.
  Chapters 4 (IPC/sockets), 5 (Remote Invocation), 6 (Indirect Communication),
  7 (OS Support).

- Negnevitsky, M. (2005).
  *Artificial Intelligence: A Guide to Intelligent Systems* (2nd ed.).
  Pearson Education Limited.
  Chapter 6: Artificial Neural Networks (pp. 184-232).

- Glorot, X., & Bengio, Y. (2010). Understanding the difficulty of training
  deep feedforward neural networks. *Proceedings of AISTATS*.
  *(Justification for ReLU activation over sigmoid in hidden layers)*

- Kingma, D. P., & Ba, J. (2014). Adam: A method for stochastic optimization.
  *arXiv:1412.6980*.
  *(Justification for Adam optimizer — adaptive learning rate and momentum)*

---

*Borrowed code: None. All code written by the group.*
*Dataset: Synthetically generated using physics-based PV and wind power curve models.*
