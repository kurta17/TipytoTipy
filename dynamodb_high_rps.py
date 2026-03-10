"""
Global Data-Intensive Project — Part 03
High RPS OLTP Operations with AWS DynamoDB

TipyToTipy — P2P Lending Platform
Entity: PaymentSchedule  (monthly loan repayment installments)

Table schema (mirrors models.py PaymentSchedule)
-------------------------------------------------
  PK  loan_id     (String)  — partition key
  SK  schedule_id (String)  — sort key
  Attributes:
    installment_number  — instalment index (1, 2, 3 …)
    due_date            — ISO date string
    amount_due          — Decimal
    principal_portion   — Decimal
    interest_portion    — Decimal
    status              — scheduled | paid | missed | defaulted
    paid_at             — ISO datetime (set when paid)
    from_wallet_id      — borrower wallet (denormalised)
    to_wallet_id        — lender wallet   (denormalised)
    borrower_id         — denormalised for fast lookup
    lender_id           — denormalised for fast lookup
    updated_at          — ISO datetime

Functions
---------
  1. insert_payment_schedule(...)       — insert a single PaymentSchedule record
  2. perf_test_insert(n)               — insert n records concurrently, report RPS
  3. update_schedule_status(...)        — update status (+ paid_at) of a record
  4. perf_test_update(schedule_keys)   — update a list of records concurrently, report RPS
"""

import boto3
import uuid
import time
import random
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from decimal import Decimal

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
AWS_REGION  = "ap-southeast-7"
TABLE_NAME  = "PaymentSchedules"
MAX_WORKERS = 200   # concurrent threads for load tests

dynamodb        = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_client = boto3.client  ("dynamodb", region_name=AWS_REGION)
table           = dynamodb.Table(TABLE_NAME)

VALID_STATUSES = {"scheduled", "paid", "missed", "defaulted"}


# ─────────────────────────────────────────────
# TABLE BOOTSTRAP  (idempotent — skip if exists)
# ─────────────────────────────────────────────
def ensure_table_exists() -> None:
    """Create the PaymentSchedules table if it does not already exist."""
    existing = [t.name for t in dynamodb.tables.all()]
    if TABLE_NAME in existing:
        print(f"[setup] Table '{TABLE_NAME}' already exists.")
        return

    print(f"[setup] Creating table '{TABLE_NAME}' …")
    dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "loan_id",     "KeyType": "HASH"},
            {"AttributeName": "schedule_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "loan_id",     "AttributeType": "S"},
            {"AttributeName": "schedule_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",  # on-demand — scales to any RPS automatically
    )
    table.wait_until_exists()
    print(f"[setup] Table '{TABLE_NAME}' is ACTIVE.")


# ─────────────────────────────────────────────
# 1. INSERT — single record
# ─────────────────────────────────────────────
def insert_payment_schedule(
    loan_id:            str,
    schedule_id:        str | None  = None,
    borrower_id:        str | None  = None,
    lender_id:          str | None  = None,
    from_wallet_id:     str | None  = None,
    to_wallet_id:       str | None  = None,
    installment_number: int         = 1,
    amount_due:         float       = 0.0,
    principal_portion:  float       = 0.0,
    interest_portion:   float       = 0.0,
    status:             str         = "scheduled",
    due_date:           str | None  = None,
) -> dict:
    """
    Insert a single PaymentSchedule record into DynamoDB.

    All monetary values are stored as Decimal to avoid float precision issues.
    Returns the item dict that was written.
    """
    if status not in VALID_STATUSES:
        raise ValueError(f"status must be one of {VALID_STATUSES}, got '{status}'")

    now         = datetime.now(timezone.utc).isoformat()
    schedule_id = schedule_id or str(uuid.uuid4())

    item = {
        "loan_id":            loan_id,
        "schedule_id":        schedule_id,
        "borrower_id":        borrower_id    or str(uuid.uuid4()),
        "lender_id":          lender_id      or str(uuid.uuid4()),
        "from_wallet_id":     from_wallet_id or str(uuid.uuid4()),
        "to_wallet_id":       to_wallet_id   or str(uuid.uuid4()),
        "installment_number": installment_number,
        "amount_due":         Decimal(str(round(amount_due,        2))),
        "principal_portion":  Decimal(str(round(principal_portion, 2))),
        "interest_portion":   Decimal(str(round(interest_portion,  2))),
        "status":             status,
        "due_date":           due_date or now[:10],
        "updated_at":         now,
    }

    table.put_item(Item=item)
    return item


# ─────────────────────────────────────────────
# 2. PERFORMANCE TEST — inserts
# ─────────────────────────────────────────────
def perf_test_insert(n: int = 50_000) -> list[dict]:
    """
    Insert `n` PaymentSchedule records as fast as possible using a thread pool.

    Data is pre-generated before the timer starts so only network I/O is measured.
    Prints throughput (RPS) and returns inserted keys for the update perf-test.
    """
    print(f"\n[insert-perf] Starting insert of {n:,} records …")

    today = date.today()

    # Pre-generate all values outside the timed region
    loan_ids      = [str(uuid.uuid4()) for _ in range(n)]
    principals    = [round(random.uniform(100, 4000), 2) for _ in range(n)]
    interest_rate = 0.08 / 12  # simulate ~8% annual / monthly
    due_dates     = [
        (today + timedelta(days=30 * random.randint(1, 36))).isoformat()
        for _ in range(n)
    ]
    installments  = [random.randint(1, 36) for _ in range(n)]

    inserted_keys: list[dict] = []
    errors = 0

    def _insert(i: int) -> dict | None:
        try:
            p = principals[i]
            interest = round(p * interest_rate, 2)
            item = insert_payment_schedule(
                loan_id            = loan_ids[i],
                installment_number = installments[i],
                amount_due         = round(p + interest, 2),
                principal_portion  = p,
                interest_portion   = interest,
                status             = "scheduled",
                due_date           = due_dates[i],
            )
            return {"loan_id": item["loan_id"], "schedule_id": item["schedule_id"]}
        except ClientError:
            return None

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_insert, i): i for i in range(n)}
        for future in as_completed(futures):
            result = future.result()
            if result:
                inserted_keys.append(result)
            else:
                errors += 1

    elapsed = time.perf_counter() - t0
    success = len(inserted_keys)
    rps     = success / elapsed

    print(f"[insert-perf] Done in {elapsed:.2f}s")
    print(f"[insert-perf] Inserted : {success:,}  |  Errors: {errors:,}")
    print(f"[insert-perf] Throughput: {rps:,.0f} RPS")
    print(f"[insert-perf] Sample inserted key: {inserted_keys[0:3] if inserted_keys else 'N/A'}")
    return inserted_keys


# ─────────────────────────────────────────────
# 3. UPDATE — single record
# ─────────────────────────────────────────────
def update_schedule_status(
    loan_id:     str,
    schedule_id: str,
    new_status:  str,
    paid_at:     str | None = None,
) -> dict:
    """
    Update the `status` (and `paid_at` when marking as paid) of a PaymentSchedule.

    Uses a ConditionExpression to guard against updating non-existent items.
    Returns the updated attribute values.
    """
    if new_status not in VALID_STATUSES:
        raise ValueError(f"status must be one of {VALID_STATUSES}, got '{new_status}'")

    now    = datetime.now(timezone.utc).isoformat()
    paid_at = paid_at or (now if new_status == "paid" else None)

    update_expr = "SET #s = :s, updated_at = :u"
    expr_names  = {"#s": "status"}
    expr_values = {":s": new_status, ":u": now}

    if paid_at:
        update_expr += ", paid_at = :p"
        expr_values[":p"] = paid_at

    response = table.update_item(
        Key={"loan_id": loan_id, "schedule_id": schedule_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
        ConditionExpression="attribute_exists(loan_id)",
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


# ─────────────────────────────────────────────
# 4. PERFORMANCE TEST — updates
# ─────────────────────────────────────────────
def perf_test_update(schedule_keys: list[dict]) -> None:
    """
    Update every key in `schedule_keys` concurrently, simulating a payment-processing run.

    `schedule_keys` is a list of {"loan_id": ..., "schedule_id": ...} dicts
    (as returned by perf_test_insert).
    """
    n = len(schedule_keys)
    print(f"\n[update-perf] Starting update of {n:,} records …")

    statuses = ["paid", "missed", "defaulted", "scheduled"]
    errors   = 0
    success  = 0

    def _update(key: dict) -> bool:
        try:
            update_schedule_status(
                loan_id     = key["loan_id"],
                schedule_id = key["schedule_id"],
                new_status  = random.choice(statuses),
            )
            return True
        except ClientError:
            return False

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_update, k) for k in schedule_keys]
        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                errors += 1

    elapsed = time.perf_counter() - t0
    rps     = success / elapsed

    print(f"[update-perf] Done in {elapsed:.2f}s")
    print(f"[update-perf] Updated : {success:,}  |  Errors: {errors:,}")
    print(f"[update-perf] Throughput: {rps:,.0f} RPS")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # 0. Create table if needed
    ensure_table_exists()

    # 1. Single insert smoke-test
    print("\n[smoke] Inserting one PaymentSchedule …")
    sample = insert_payment_schedule(
        loan_id            = str(uuid.uuid4()),
        installment_number = 1,
        amount_due         = 345.67,
        principal_portion  = 320.00,
        interest_portion   = 25.67,
        status             = "scheduled",
        due_date           = "2026-04-01",
    )
    print(f"[smoke] Inserted: loan_id={sample['loan_id']}  schedule_id={sample['schedule_id']}")

    # 2. Single update smoke-test
    print("\n[smoke] Marking that instalment as 'paid' …")
    updated = update_schedule_status(
        loan_id     = sample["loan_id"],
        schedule_id = sample["schedule_id"],
        new_status  = "paid",
    )
    print(f"[smoke] Updated attributes: {updated}")

    # 3. Insert performance test  (50 000 records)
    inserted_keys = perf_test_insert(n=50_000)

    # 4. Update performance test  (all records just inserted)
    perf_test_update(schedule_keys=inserted_keys)
