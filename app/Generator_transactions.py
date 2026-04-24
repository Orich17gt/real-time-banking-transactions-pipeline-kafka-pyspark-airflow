import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

# -----------------------------------
# CONFIG
# -----------------------------------
OUTPUT_FILE = "/data/raw/raw_transactions.ndjson"
NUM_TRANSACTIONS = 25000

NUM_CUSTOMERS = 4000
NUM_ACCOUNTS = 7000

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 3, 31)

FRAUD_RATE_MIN = 0.05
FRAUD_RATE_MAX = 0.10

TXN_TYPES = ["debit", "credit"]
TXN_STATUSES = ["success", "failed", "pending", "settled"]
CREDIT_RISK_LEVELS = ["low", "medium", "high"]

TXN_TYPE_WEIGHTS = [0.72, 0.28]
TXN_STATUS_WEIGHTS = [0.45, 0.08, 0.17, 0.30]
CREDIT_RISK_WEIGHTS = [0.60, 0.28, 0.12]

CHANNELS = ["mobile", "web", "atm", "branch", "pos"]
CHANNEL_WEIGHTS = [0.30, 0.18, 0.12, 0.05, 0.35]

MERCHANT_CATEGORIES = [
    "grocery", "gas", "restaurant", "retail", "utilities",
    "salary", "transfer", "mortgage", "subscription", "travel"
]
MERCHANT_WEIGHTS = [0.18, 0.10, 0.12, 0.14, 0.07, 0.08, 0.10, 0.05, 0.06, 0.10]

# -----------------------------------
# HELPERS
# -----------------------------------
def random_datetime(start_dt: datetime, end_dt: datetime) -> datetime:
    delta_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, delta_seconds))


def generate_customer_ids(n: int) -> List[str]:
    return [f"CUST{str(i).zfill(6)}" for i in range(1, n + 1)]


def generate_account_ids(n: int) -> List[str]:   # ✅ FIXED
    return [f"ACCT{str(i).zfill(8)}" for i in range(1, n + 1)]


def build_account_customer_map(account_ids: List[str], customer_ids: List[str]) -> Dict[str, str]:
    return {acct: random.choice(customer_ids) for acct in account_ids}


def choose_credit_risk() -> str:
    return random.choices(CREDIT_RISK_LEVELS, weights=CREDIT_RISK_WEIGHTS, k=1)[0]


def choose_txn_type() -> str:
    return random.choices(TXN_TYPES, weights=TXN_TYPE_WEIGHTS, k=1)[0]


def choose_txn_status(txn_type: str, fraud_flag: bool) -> str:
    if fraud_flag:
        return random.choices(
            ["success", "failed", "pending", "settled"],
            weights=[0.12, 0.28, 0.35, 0.25],
            k=1
        )[0]
    return random.choices(TXN_STATUSES, weights=TXN_STATUS_WEIGHTS, k=1)[0]


def choose_channel() -> str:
    return random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]


def choose_merchant_category(txn_type: str) -> str:
    if txn_type == "credit":
        return random.choices(
            ["salary", "transfer", "refund", "deposit"],
            weights=[0.45, 0.35, 0.10, 0.10],
            k=1
        )[0]
    return random.choices(MERCHANT_CATEGORIES, weights=MERCHANT_WEIGHTS, k=1)[0]


def generate_txn_amount(txn_type: str, fraud_flag: bool, credit_risk: str) -> float:
    if txn_type == "debit":
        if fraud_flag:
            amount = random.uniform(800, 5000)
        elif credit_risk == "high":
            amount = random.uniform(20, 2500)
        else:
            amount = random.uniform(5, 1200)
    else:
        if fraud_flag:
            amount = random.uniform(500, 3000)
        else:
            amount = random.uniform(20, 4000)

    return round(amount, 2)


def generate_account_balance(txn_type: str, txn_amount: float, txn_status: str) -> float:
    base_balance = random.uniform(200, 25000)

    if txn_status in {"failed", "pending"}:
        return round(base_balance, 2)

    if txn_type == "debit":
        return round(max(0, base_balance - txn_amount), 2)
    else:
        return round(base_balance + txn_amount, 2)


def generate_settlement_date(txn_dt: datetime, txn_status: str) -> Optional[str]:  # ✅ FIXED
    if txn_status == "failed":
        return None

    if txn_status == "pending":
        if random.random() < 0.65:
            return None
        return (txn_dt + timedelta(hours=random.randint(6, 48))).isoformat()

    return (txn_dt + timedelta(hours=random.randint(1, 72))).isoformat()


def generate_raw_transaction(account_customer_map: Dict[str, str], fraud_rate: float) -> Dict:  # ✅ FIXED
    account_id = random.choice(list(account_customer_map.keys()))
    customer_id = account_customer_map[account_id]

    txn_dt = random_datetime(START_DATE, END_DATE)
    fraud_flag = random.random() < fraud_rate

    credit_risk = choose_credit_risk()
    txn_type = choose_txn_type()
    txn_status = choose_txn_status(txn_type, fraud_flag)

    txn_amount = generate_txn_amount(txn_type, fraud_flag, credit_risk)
    settlement_date = generate_settlement_date(txn_dt, txn_status)
    account_balance = generate_account_balance(txn_type, txn_amount, txn_status)

    return {
        "event_id": str(uuid.uuid4()),
        "ingest_ts": datetime.utcnow().isoformat(),
        "source_system": "core_banking_simulator",
        "transaction_id": f"TXN{uuid.uuid4().hex[:16].upper()}",
        "account_id": account_id,
        "customer_id": customer_id,
        "txn_date": txn_dt.isoformat(),
        "txn_type": txn_type,
        "txn_amount": txn_amount,
        "txn_status": txn_status,
        "settlement_date": settlement_date,
        "account_balance": account_balance,
        "fraud_flag": fraud_flag,
        "credit_risk": credit_risk,
        "channel": choose_channel(),
        "merchant_category": choose_merchant_category(txn_type),
        "currency": "USD",
        "country_code": "US"
    }


# -----------------------------------
# MAIN
# -----------------------------------
def main():
    random.seed(42)

    fraud_rate = random.uniform(FRAUD_RATE_MIN, FRAUD_RATE_MAX)

    customer_ids = generate_customer_ids(NUM_CUSTOMERS)
    account_ids = generate_account_ids(NUM_ACCOUNTS)

    account_customer_map = build_account_customer_map(account_ids, customer_ids)

    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w") as f:
        for _ in range(NUM_TRANSACTIONS):
            record = generate_raw_transaction(account_customer_map, fraud_rate)
            f.write(json.dumps(record) + "\n")

    print("✅ File generated successfully")


if __name__ == "__main__":
    main()