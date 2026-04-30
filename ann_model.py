"""
ann_model.py - Artificial Neural Network for Renewable Energy Output Prediction
Group Project - Distributed Systems (Option 2) + Intelligent Systems

This module defines and trains a multilayer feedforward neural network
that predicts energy output (kWh) from weather sensor features.

The network architecture and training algorithm are grounded in:
    Negnevitsky, M. (2005). Artificial Intelligence: A Guide to Intelligent
    Systems (2nd ed.). Pearson Education Limited.

    - Section 6.1 (p.184): Introduction to neural networks and machine learning
    - Section 6.2 (p.188): The neuron as a computing element; weighted sum
                            and activation functions (Eq. 6.1, 6.2, Fig. 6.3)
    - Section 6.4 (p.194): Multilayer networks; backpropagation algorithm
                            Steps 1-4; error gradient (Eq. 6.10 - 6.15)
    - Section 6.5 (p.204): Accelerated learning; momentum term (Eq. 6.17);
                            adaptive learning rate heuristics

Input features (5 neurons in input layer):
    irradiance   (W/m²)  - solar irradiance at sensor
    temperature  (°C)    - ambient temperature
    humidity     (%)     - relative humidity
    wind_speed   (m/s)   - wind speed at hub height
    hour         (0-23)  - hour of the day

Target:
    energy_kwh   (kWh)   - combined renewable energy output

Architecture:
    Input (5) → Dense 10, ReLU → Dense 8, ReLU → Output 1, Linear

Note on activation function choice:
    Negnevitsky §6.2 and §6.4 use the sigmoid activation function (Eq. 6.9).
    This implementation uses ReLU (Rectified Linear Unit) for hidden layers,
    a modern improvement that avoids the vanishing gradient problem that
    sigmoid and tanh functions suffer in deeper networks (Glorot & Bengio, 2010).
    The underlying backpropagation mathematics (§6.4) remain the same -
    only the derivative of the activation function changes during the
    error gradient calculation (Eq. 6.13).
"""

import os
import pickle
import numpy as np

try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers
    from tensorflow.keras.callbacks import EarlyStopping
    KERAS_AVAILABLE = True
except ImportError:
    KERAS_AVAILABLE = False
    print("[ANN] WARNING: TensorFlow not installed. "
          "Install it with: pip install tensorflow")


class RenewableEnergyANN:
    """
    Multilayer feedforward neural network for regression on renewable energy data.

    Design follows the network description in Negnevitsky §6.4, Figure 6.8:
    an input layer, one or more hidden layers, and an output layer.
    The backpropagation algorithm (§6.4 Steps 1-4) handles weight updates.
    """

    # File names for saving/loading the trained model
    MODEL_FILE = 'ann_model_weights.keras'
    SCALER_FILE = 'ann_scalers.pkl'

    # Feature names - used for display and validation
    FEATURE_NAMES = ['irradiance', 'temperature', 'humidity', 'wind_speed', 'hour']
    N_FEATURES = 5

    def __init__(self):
        self.model = None
        self.scaler_X = None   # normaliser for input features
        self.scaler_y = None   # normaliser for target values
        self.training_history = None
        self._is_trained = False

    # Build
    

    def build(self):
        """
        Construct the neural network architecture.

        Following Negnevitsky §6.4 (p.194):
            'A multilayer perceptron is a feedforward neural network with
             one or more hidden layers.'

        Layer design rationale:
            Input (5):   one neuron per feature - no computation, just distribution
                         (Negnevitsky §6.4: 'The input layer accepts input signals
                          and redistributes these signals to neurons in the hidden layer')
            Hidden 1 (10, ReLU): detects lower-level feature patterns
            Hidden 2 (8, ReLU):  detects higher-level combinations
            Output (1, Linear):  continuous regression output in kWh

        Negnevitsky §6.4 (p.195):
            'With one hidden layer we can represent any continuous function
             of the input signals.'
        """
        if not KERAS_AVAILABLE:
            raise RuntimeError("TensorFlow/Keras is required. See requirements.txt.")

        model = keras.Sequential(name='renewable_energy_ann')

        # Input layer shape declaration
        model.add(layers.Input(shape=(self.N_FEATURES,), name='input_layer'))

        # Hidden Layer 1 - 10 neurons
        # Negnevitsky §6.4: hidden neurons 'detect the features hidden in the input patterns'
        model.add(layers.Dense(
            units=10,
            activation='relu',
            name='hidden_layer_1',
            # Glorot uniform initialisation - analogous to Negnevitsky §6.4 Step 1:
            # 'Set all weights to random numbers in range [-2.4/Fi, +2.4/Fi]'
            kernel_initializer='glorot_uniform',
            bias_initializer='zeros'
        ))

        # Hidden Layer 2 - 8 neurons
        model.add(layers.Dense(
            units=8,
            activation='relu',
            name='hidden_layer_2',
            kernel_initializer='glorot_uniform',
            bias_initializer='zeros'
        ))

        # Output Layer - 1 neuron, linear activation for regression
        model.add(layers.Dense(
            units=1,
            activation='linear',
            name='output_layer'
        ))

        # Compile with Adam optimiser
        # Adam implements adaptive per-parameter learning rates and momentum,
        # which is conceptually related to Negnevitsky §6.5 (Eq. 6.17):
        #     Δw_jk(p) = β * Δw_jk(p-1) + α * y_j(p) * δ_k(p)
        # where β is the momentum constant and α is the learning rate.
        # Adam extends this idea with adaptive moment estimation.
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss='mse',    # Mean Squared Error - minimising SSE per §6.4 Step 4
            metrics=['mae']
        )

        self.model = model
        return model

    def summary(self):
        """Print the network architecture summary."""
        if self.model is None:
            print("[ANN] Model not built yet. Call build() first.")
        else:
            self.model.summary()

   
    # Normalisation
   

    def _fit_scalers(self, X, y):
        """
        Fit MinMaxScaler to training data and scale X and y.

        Normalisation to [0, 1] is important because large raw values
        cause proportionally large weight updates that destabilise training.
        This relates to the weight initialisation advice in Negnevitsky §6.4
        Step 1, which assumes inputs are bounded.
        """
        from sklearn.preprocessing import MinMaxScaler

        self.scaler_X = MinMaxScaler(feature_range=(0, 1))
        self.scaler_y = MinMaxScaler(feature_range=(0, 1))

        X_scaled = self.scaler_X.fit_transform(X)
        y_scaled = self.scaler_y.fit_transform(y.reshape(-1, 1)).ravel()
        return X_scaled, y_scaled

    def _scale_X(self, X):
        if self.scaler_X is None:
            raise ValueError("[ANN] Scaler not fitted. Train the model first.")
        return self.scaler_X.transform(X)

    def _unscale_y(self, y_scaled):
        return self.scaler_y.inverse_transform(y_scaled.reshape(-1, 1))

    
    # Training
   

    def train(self, X_train, y_train, epochs=300, batch_size=32, val_split=0.2):
        """
        Train the network using backpropagation.

        Algorithm maps to Negnevitsky §6.4 Steps 1-4:

            Step 1 - Initialisation (handled by Keras at model build time):
                Weights set to small random values in [-2.4/Fi, +2.4/Fi]

            Step 2 - Activation (forward pass per batch):
                y_j = ReLU( Σ x_i * w_ij - θ_j )   for hidden neurons
                y_k = linear( Σ y_j * w_jk - θ_k )  for output neuron
                (Eq. 6.6 uses step, we use ReLU; sigmoid used in §6.4 Eq. 6.9)

            Step 3 - Weight training (backpropagation):
                Compute error gradient δ_k for output layer (Eq. 6.13, 6.14)
                Propagate δ_j to hidden layer (Eq. 6.15)
                Update weights: w(p+1) = w(p) + Δw(p)  (Eq. 6.11)
                Adam optimizer handles this with momentum and adaptive lr (§6.5)

            Step 4 - Iteration:
                Repeat Steps 2-3 until val_loss stops decreasing (EarlyStopping)

        Args:
            X_train     (ndarray): shape (n, 5) - feature matrix
            y_train     (ndarray): shape (n,)   - target kWh values
            epochs      (int):    maximum iterations (Negnevitsky calls these 'epochs')
            batch_size  (int):    samples per gradient update (mini-batch)
            val_split   (float):  fraction held out for validation loss monitoring
        """
        if self.model is None:
            self.build()

        print(f"\n[ANN] === Training Started ===")
        print(f"[ANN] Samples       : {len(X_train)}")
        print(f"[ANN] Features      : {self.FEATURE_NAMES}")
        print(f"[ANN] Architecture  : 5 → 10 (ReLU) → 8 (ReLU) → 1 (Linear)")
        print(f"[ANN] Optimizer     : Adam (lr=0.001)")
        print(f"[ANN] Loss function : MSE (Sum of Squared Errors, §6.4 Step 4)")
        print(f"[ANN] Max epochs    : {epochs}, Batch size: {batch_size}\n")

        X_scaled, y_scaled = self._fit_scalers(X_train, y_train)

        # EarlyStopping: implements the convergence criterion from §6.4 Step 4
        # 'repeat the process until the selected error criterion is satisfied'
        early_stop = EarlyStopping(
            monitor='val_loss',
            patience=20,             # stop after 20 epochs with no improvement
            restore_best_weights=True,
            verbose=1
        )

        self.training_history = self.model.fit(
            X_scaled,
            y_scaled,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=val_split,
            callbacks=[early_stop],
            verbose=1
        )

        actual_epochs = len(self.training_history.history['loss'])
        final_loss = self.training_history.history['val_loss'][-1]
        print(f"\n[ANN] Training complete.")
        print(f"[ANN] Stopped at epoch : {actual_epochs}")
        print(f"[ANN] Final val loss   : {final_loss:.6f}")
        self._is_trained = True

        return self.training_history

    
    # Inference
    

    def predict(self, X):
        """
        Run a forward pass through the trained network.

        Implements forward propagation per Negnevitsky §6.4 Step 2:
            For each hidden neuron j:
                X_j = Σ x_i * w_ij - θ_j
                y_j = ReLU(X_j)

            For the output neuron k:
                X_k = Σ y_j * w_jk - θ_k
                y_k = X_k  (linear activation for regression)

        Args:
            X (ndarray): shape (n, 5) - must follow same feature order as training

        Returns:
            ndarray: shape (n, 1) - predicted energy output in kWh
        """
        if self.model is None or not self._is_trained:
            raise RuntimeError("[ANN] Model not trained. Run train() or load_model() first.")

        X_scaled = self._scale_X(X)
        y_scaled = self.model.predict(X_scaled, verbose=0)
        return self._unscale_y(y_scaled)

    
    # Evaluation
  

    def evaluate(self, X_test, y_test):
        """
        Compute standard regression metrics on a held-out test set.

        RMSE relates directly to the MSE loss minimised during training
        (Negnevitsky §6.4: 'repeat until the selected error criterion is satisfied').
        R² measures how much variance the network explains.

        Args:
            X_test  (ndarray): shape (n, 5)
            y_test  (ndarray): shape (n,)

        Returns:
            dict: RMSE, MAE, R² values
        """
        preds = self.predict(X_test).ravel()

        rmse = float(np.sqrt(np.mean((y_test - preds) ** 2)))
        mae = float(np.mean(np.abs(y_test - preds)))

        ss_res = float(np.sum((y_test - preds) ** 2))
        ss_tot = float(np.sum((y_test - np.mean(y_test)) ** 2))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        return {
            'RMSE': round(rmse, 5),
            'MAE':  round(mae, 5),
            'R2':   round(r2, 5)
        }

    
    # Persistence
    

    def save_model(self):
        """Save model weights and scalers to disk for later use."""
        if self.model is None:
            print("[ANN] Nothing to save - model has not been built.")
            return

        self.model.save(self.MODEL_FILE)

        with open(self.SCALER_FILE, 'wb') as f:
            pickle.dump({
                'scaler_X': self.scaler_X,
                'scaler_y': self.scaler_y
            }, f)

        print(f"[ANN] Model saved to '{self.MODEL_FILE}'")
        print(f"[ANN] Scalers saved to '{self.SCALER_FILE}'")

    def load_model(self):
        """
        Load a previously saved model and scalers.
        Returns True if successful, False otherwise.
        """
        if not KERAS_AVAILABLE:
            return False

        if not (os.path.exists(self.MODEL_FILE) and os.path.exists(self.SCALER_FILE)):
            return False

        try:
            self.model = keras.models.load_model(self.MODEL_FILE)
            with open(self.SCALER_FILE, 'rb') as f:
                scalers = pickle.load(f)
            self.scaler_X = scalers['scaler_X']
            self.scaler_y = scalers['scaler_y']
            self._is_trained = True
            print(f"[ANN] Model loaded from '{self.MODEL_FILE}'")
            return True
        except Exception as e:
            print(f"[ANN] Could not load model: {e}")
            return False



# Training script - run this directly to train and save the model


if __name__ == '__main__':
    import pandas as pd

    DATASET = 'renewable_energy_dataset.csv'

    if not os.path.exists(DATASET):
        print(f"[ANN] Dataset file '{DATASET}' not found.")
        print("[ANN] Generate it first:  python dataset_generator.py")
        exit(1)

    print("[ANN] Loading dataset...")
    df = pd.read_csv(DATASET)
    print(f"[ANN] Loaded {len(df)} rows from '{DATASET}'")
    print()
    print(df[['irradiance', 'temperature', 'humidity', 'wind_speed', 'hour', 'energy_kwh']].describe().round(3))
    print()

    feature_cols = ['irradiance', 'temperature', 'humidity', 'wind_speed', 'hour']
    X = df[feature_cols].values
    y = df['energy_kwh'].values

    # Chronological train/test split - do NOT shuffle time-series data
    # Using the last 20% as a held-out test set avoids data leakage
    split = int(len(X) * 0.80)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    print(f"[ANN] Train: {len(X_train)} samples | Test: {len(X_test)} samples")
    print(f"[ANN] Split at index {split} (chronological - no shuffle)\n")

    ann = RenewableEnergyANN()
    ann.build()
    ann.summary()

    ann.train(X_train, y_train, epochs=300, batch_size=32, val_split=0.15)

    results = ann.evaluate(X_test, y_test)
    print("\n[ANN] === Test Set Results ===")
    print(f"  RMSE : {results['RMSE']} kWh")
    print(f"  MAE  : {results['MAE']} kWh")
    print(f"  R²   : {results['R2']}")
    print()

    ann.save_model()
    print("\n[ANN] Done. The trained model is ready for use in subscriber.py --mode ann")
