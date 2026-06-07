"""Sample data generator — adapted from the spec (Capstone_Project_Team_1.md, lines 593-655).

The intentional-defect logic (duplicate IDs, nulls, invalid emails/dates/timestamps,
orphan user/product ID ranges) is kept VERBATIM: that is the whole point of this
generator — it gives the pipeline real anomalies to detect, quarantine, and report on.

Run once via:  python -m etl.generate_sample_data [--out data/raw] [--seed 42]
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker


def generate(output_dir: str = "data/raw", seed: int = 42) -> dict[str, str]:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    np.random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)

    # ─── Users ───────────────────────────────────────────────
    users = []
    for i in range(1, 1101):
        users.append({
            "user_id": i if random.random() > 0.03 else random.randint(1, 50),  # intentional duplicates
            "user_name": fake.name() if random.random() > 0.05 else None,
            "email": fake.email() if random.random() > 0.1 else "invalid_email",
            "signup_date": fake.date_between(start_date="-5y", end_date="today")
                           if random.random() > 0.05 else "invalid_date"
        })
    users_df = pd.DataFrame(users)
    users_path = os.path.join(output_dir, "users.csv")
    users_df.to_csv(users_path, index=False)

    # ─── Products ────────────────────────────────────────────
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
    products = []
    for i in range(1, 1101):
        products.append({
            "product_id": i if random.random() > 0.02 else random.randint(1, 100),
            "product_name": fake.word().title(),
            "category": random.choice(categories),
            "price": round(random.uniform(5, 500), 2) if random.random() > 0.1 else None
        })
    products_df = pd.DataFrame(products)
    products_path = os.path.join(output_dir, "products.csv")
    products_df.to_csv(products_path, index=False)

    # ─── Web Logs ────────────────────────────────────────────
    actions = ["view", "add_to_cart", "purchase"]
    weblogs = []
    for i in range(1, 15001):
        user_id = random.randint(1, 1200)      # range exceeds users table → orphans
        product_id = random.randint(1, 1200)   # range exceeds products table → orphans
        session_id = f"sess_{random.randint(1, 5000)}" if random.random() > 0.05 else None
        timestamp = datetime.now() - timedelta(
            days=random.randint(0, 365),
            seconds=random.randint(0, 86400)
        )
        if random.random() < 0.05:
            timestamp = "invalid_timestamp"    # intentional bad timestamps

        weblogs.append({
            "log_id": i if random.random() > 0.03 else random.randint(1, 200),
            "user_id": user_id if random.random() > 0.05 else None,
            "product_id": product_id,
            "session_id": session_id,
            "action": random.choice(actions),
            "timestamp": timestamp
        })
    weblogs_df = pd.DataFrame(weblogs)
    weblogs_path = os.path.join(output_dir, "weblogs.csv")
    weblogs_df.to_csv(weblogs_path, index=False)

    return {"users": users_path, "products": products_path, "weblogs": weblogs_path}


def _main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample web log CSVs (with intentional defects)")
    parser.add_argument("--out", default="data/raw", help="Output directory (default: data/raw)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    paths = generate(output_dir=args.out, seed=args.seed)
    for name, path in paths.items():
        df = pd.read_csv(path)
        print(f"{name}: {len(df):>6} rows -> {path}")
    print("Web log CSV files generated successfully!")


if __name__ == "__main__":
    _main()
