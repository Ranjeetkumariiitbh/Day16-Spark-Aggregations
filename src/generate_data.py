"""
generate_data.py
-----------------
Generates a synthetic hospital billing dataset used by hospital_aggregations.py.

Run this only if you want to regenerate data/hospital_billing.csv with a
different size or seed. A ready-made CSV is already included in data/.

Usage:
    python src/generate_data.py --rows 500 --seed 42
"""

import argparse
import csv
import random
from datetime import date, timedelta

DEPARTMENTS = [
    "Cardiology", "Orthopedics", "Neurology", "Pediatrics",
    "Oncology", "General Medicine", "Dermatology", "ENT",
]

DOCTORS_BY_DEPT = {
    "Cardiology": ["Dr. Rao", "Dr. Mehta", "Dr. Iyer"],
    "Orthopedics": ["Dr. Singh", "Dr. Kapoor"],
    "Neurology": ["Dr. Das", "Dr. Nair"],
    "Pediatrics": ["Dr. Sharma", "Dr. Gupta"],
    "Oncology": ["Dr. Bose", "Dr. Chatterjee"],
    "General Medicine": ["Dr. Verma", "Dr. Reddy", "Dr. Joshi"],
    "Dermatology": ["Dr. Menon"],
    "ENT": ["Dr. Pillai", "Dr. Khan"],
}

# rough bill ranges per department (min, max) in INR
BILL_RANGE = {
    "Cardiology": (15000, 250000),
    "Orthopedics": (10000, 180000),
    "Neurology": (12000, 220000),
    "Pediatrics": (2000, 40000),
    "Oncology": (20000, 400000),
    "General Medicine": (1500, 25000),
    "Dermatology": (1000, 15000),
    "ENT": (2000, 30000),
}

PAYMENT_MODES = ["Cash", "Card", "Insurance", "UPI"]
CITIES = ["Hyderabad", "Bengaluru", "Chennai", "Mumbai", "Pune", "Delhi"]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_rows(n: int):
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)
    rows = []
    for i in range(1, n + 1):
        dept = random.choice(DEPARTMENTS)
        doctor = random.choice(DOCTORS_BY_DEPT[dept])
        low, high = BILL_RANGE[dept]
        bill_amount = round(random.uniform(low, high), 2)
        admission = random_date(start, end)
        stay_days = random.randint(0, 10)
        discharge = admission + timedelta(days=stay_days)
        rows.append(
            {
                "transaction_id": f"TXN{i:05d}",
                "patient_id": f"PAT{random.randint(1, n // 2):05d}",
                "department": dept,
                "doctor_name": doctor,
                "bill_amount": bill_amount,
                "payment_mode": random.choice(PAYMENT_MODES),
                "city": random.choice(CITIES),
                "admission_date": admission.isoformat(),
                "discharge_date": discharge.isoformat(),
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic hospital billing data")
    parser.add_argument("--rows", type=int, default=500, help="number of rows to generate")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    parser.add_argument("--out", type=str, default="data/hospital_billing.csv", help="output CSV path")
    args = parser.parse_args()

    random.seed(args.seed)
    rows = generate_rows(args.rows)

    fieldnames = list(rows[0].keys())
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
