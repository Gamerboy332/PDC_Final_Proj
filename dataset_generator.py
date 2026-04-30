"""
dataset_generator.py - Synthetic Renewable Energy Dataset Generator
Group Project - Distributed Systems (Option 2) + Intelligent Systems

Generates a CSV dataset of hourly renewable energy readings suitable for
training the ANN model in ann_model.py.

Simulates one year of hourly data (365 days x 24 hours = 8,760 rows).

Columns generated:
    date          - calendar date (YYYY-MM-DD)
    hour          - hour of day (0-23)
    irradiance    - solar irradiance (W/m²), zero at night
    temperature   - ambient temperature (°C), correlated with irradiance
    humidity      - relative humidity (%), inversely related to temperature
    wind_speed    - wind speed (m/s), random with slight nocturnal boost
    energy_kwh    - combined solar + wind energy output (kWh) - the target

The physics-based formulas are simplified but follow the same feature
relationships used in the ANN model. Adding some random noise makes the
dataset realistic and prevents the network from perfectly memorising it.

Usage:
    python dataset_generator.py
    python dataset_generator.py --days 180 --output my_data.csv
"""

import numpy as np
import pandas as pd
import argparse
import math
from datetime import datetime, timedelta


def generate(n_days=365, seed=42, output_file='renewable_energy_dataset.csv'):
    """
    Generate n_days * 24 hourly records and save them to output_file.

    Args:
        n_days      (int):  how many days of data to generate
        seed        (int):  random seed for reproducibility
        output_file (str):  CSV output path

    Returns:
        pd.DataFrame: the generated dataset
    """
    np.random.seed(seed)
    start = datetime(2024, 1, 1, 0, 0, 0)
    records = []

    for day in range(n_days):
        current_day = start + timedelta(days=day)
        month = current_day.month

        # Seasonal factor: peaks in summer (month 6), troughs in winter
        # Simple sinusoidal model centred on June (month 6)
        season = 1.0 + 0.30 * math.sin(2 * math.pi * (month - 3) / 12)

        # Cloud cover factor - varies day to day
        cloud_cover = np.random.uniform(0.6, 1.0)

        for hour in range(24):

         
            # Solar irradiance
           
            # Gaussian bell curve peaking at solar noon (hour 12)
            if 6 <= hour <= 19:
                clear_sky = 950.0 * season * cloud_cover
                irradiance = clear_sky * math.exp(-0.5 * ((hour - 12) / 3.2) ** 2)
                irradiance += np.random.normal(0, 45)
                irradiance = max(0.0, irradiance)
            else:
                irradiance = 0.0

            
            # Temperature (°C)
            
            # Baseline depends on season; irradiance adds daytime heating
            baseline_temp = 12.0 + season * 7.0
            temperature = baseline_temp + (irradiance / 120) * 0.3
            temperature += np.random.normal(0, 1.8)

            
            # Humidity (%)
            
            # Roughly inversely related to temperature
            humidity = 85.0 - temperature * 0.55
            humidity += np.random.normal(0, 6)
            humidity = float(np.clip(humidity, 15, 100))

            
            # Wind speed (m/s)
            
            # Slightly faster at night; day-to-day variation via Weibull-like spread
            base_wind = np.random.gamma(shape=2.0, scale=3.2)
            night_boost = 1.2 if (hour < 6 or hour > 21) else 0.0
            wind_speed = max(0.0, base_wind + night_boost + np.random.normal(0, 0.8))

            
            # Energy output (kWh)  -- this is the prediction target
            

            # Solar contribution
            # Based on: P = G * η * A * (1 - β*(T - 25))
            #   G = irradiance (W/m²), η = efficiency, A = area (m²)
            #   β = temperature coefficient of power
            panel_efficiency = 0.175
            panel_area_m2 = 25.0
            temp_coeff = 0.0040

            solar_gross = (irradiance * panel_efficiency * panel_area_m2) / 1000.0
            temp_derating = 1.0 - temp_coeff * max(0.0, temperature - 25.0)
            solar_kwh = max(0.0, solar_gross * temp_derating)
            solar_kwh *= np.random.uniform(0.92, 1.04)  # measurement noise

            # Wind contribution (simple power curve)
            rated_kw = 2.0
            cut_in = 3.0
            rated_speed = 12.0
            cut_out = 20.0

            if wind_speed < cut_in or wind_speed > cut_out:
                wind_kwh = 0.0
            elif wind_speed >= rated_speed:
                wind_kwh = rated_kw
            else:
                wind_kwh = rated_kw * ((wind_speed - cut_in) / (rated_speed - cut_in)) ** 3

            wind_kwh = max(0.0, wind_kwh) * np.random.uniform(0.94, 1.04)

            energy_kwh = solar_kwh + wind_kwh

            records.append({
                'date':        current_day.strftime('%Y-%m-%d'),
                'hour':        hour,
                'irradiance':  round(float(irradiance), 2),
                'temperature': round(float(temperature), 2),
                'humidity':    round(float(humidity), 2),
                'wind_speed':  round(float(wind_speed), 2),
                'energy_kwh':  round(float(energy_kwh), 4)
            })

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)

    print(f"[DataGen] Dataset generated: {len(df)} rows ({n_days} days x 24 hours)")
    print(f"[DataGen] Saved to: {output_file}")
    print()
    print("[DataGen] Summary statistics:")
    print(df[['irradiance', 'temperature', 'humidity', 'wind_speed', 'energy_kwh']].describe().round(3).to_string())

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Renewable Energy Dataset Generator')
    parser.add_argument(
        '--days', type=int, default=365,
        help='Number of days to simulate (default: 365)'
    )
    parser.add_argument(
        '--output', type=str, default='renewable_energy_dataset.csv',
        help='Output CSV filename (default: renewable_energy_dataset.csv)'
    )
    parser.add_argument(
        '--seed', type=int, default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    args = parser.parse_args()

    generate(n_days=args.days, seed=args.seed, output_file=args.output)
