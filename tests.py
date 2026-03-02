"""
tests.py — Performance benchmarks, isolation test, and demo entry point.

Responsibilities:
  • perf_test_create_users         — PERF-1: throughput of user creation
  • perf_test_process_payments     — PERF-2: throughput of payment processing
  • isolation_test_concurrent_payments — ISOLATION: concurrent wallet debits
  • main                           — end-to-end demo runner

Usage:
    export DATABASE_URL="postgresql://user:pass@localhost:5432/tipytotipy"
    python tests.py
"""

import decimal
import threading
import time
import uuid
from datetime import date, timedelta
from typing import List

from models import (
    GrantedLoan,
    LoanRequest,
    PaymentSchedule,
    PaymentStatus,
    User,
    UserRole,
    Wallet,
    create_tables,
    get_session,
)
from operations import (
    InitiatedBy,
    _build_amortized_schedule,
    op_apply_to_offer,
    op_approve_match,
    op_create_user,
    op_post_lending_offer,
    op_process_payment,
    populate_sample_data,
)

# ─── PERF-1: User creation throughput ────────────────────────────────────────────

def perf_test_create_users(n: int = 1_000) -> None:
    """
    PERF-1 — Measure throughput of op_create_user.

    Every call is an isolated ACID transaction (INSERT user + INSERT wallet).
    Increase n to stress connection pool and disk I/O.
    Default: n=1 000.  Stress run: n=100 000.
    """
    print(f"\n{'─'*60}")
    print(f"[PERF-1] Creating {n:,} users — measuring throughput...")
    print(f"{'─'*60}")

    suffix = uuid.uuid4().hex[:8]   # prevents email collisions across repeated runs
    start  = time.perf_counter()

    for i in range(n):
        op_create_user(
            full_name=f"PerfUser {i}",
            email=f"perf_{suffix}_{i}@test.com",
            password_hash="x",
            role=UserRole.borrower,
            initial_balance=decimal.Decimal("500"),
        )

    elapsed = time.perf_counter() - start
    print(f"  Completed : {n:,} users")
    print(f"  Elapsed   : {elapsed:.2f} s")
    print(f"  Throughput: {n / elapsed:,.0f} ops/sec")


# ─── PERF-2: Payment processing throughput ────────────────────────────────────────

def perf_test_process_payments(n: int = 1_000) -> None:
    """
    PERF-2 — Measure throughput of op_process_payment.

    Setup phase  (excluded from timing): creates n loans and their payment
    schedules inside one bulk transaction.
    Processing phase (timed): processes instalment #1 for every loan —
    each call is a separate ACID transaction that locks two wallet rows
    and updates three rows atomically.

    Default: n=1 000.  Stress run: n=100 000.
    """
    print(f"\n{'─'*60}")
    print(f"[PERF-2] Processing {n:,} payments — measuring throughput...")
    print(f"{'─'*60}")
    print(f"  Setting up {n:,} loans (not measured)...")

    schedule_ids: List[uuid.UUID] = []

    with get_session() as session:
        suffix = uuid.uuid4().hex[:8]

        # One shared lender with a practically unlimited wallet
        lender = User(full_name="Perf Lender",
                      email=f"perf_lender_{suffix}@test.com",
                      password_hash="x", role=UserRole.lender)
        session.add(lender)
        session.flush()

        lender_wallet = Wallet(user_id=lender.user_id,
                               balance=decimal.Decimal("999999999"),
                               currency="USD")
        session.add(lender_wallet)
        session.flush()

        for i in range(n):
            borrower = User(full_name=f"PerfBorrower {i}",
                            email=f"perf_b_{suffix}_{i}@test.com",
                            password_hash="x", role=UserRole.borrower)
            session.add(borrower)
            session.flush()

            b_wallet = Wallet(user_id=borrower.user_id,
                              balance=decimal.Decimal("5000"), currency="USD")
            session.add(b_wallet)
            session.flush()

            loan = GrantedLoan(
                borrower_id=borrower.user_id, lender_id=lender.user_id,
                principal_amount=decimal.Decimal("1000"),
                interest_rate=decimal.Decimal("8"),
                term_months=3,
                start_date=date.today(),
                end_date=date.today() + timedelta(days=90),
            )
            session.add(loan)
            session.flush()

            schedules = _build_amortized_schedule(
                loan_id=loan.loan_id,
                principal=1000.0,
                annual_rate=8.0,
                term_months=3,
                start_date=date.today(),
                from_wallet_id=b_wallet.wallet_id,
                to_wallet_id=lender_wallet.wallet_id,
            )
            session.add_all(schedules)
            session.flush()
            schedule_ids.append(schedules[0].schedule_id)  # measure instalment #1

    print(f"  Setup done. Processing {len(schedule_ids):,} payments...")

    paid_count   = 0
    missed_count = 0
    start        = time.perf_counter()

    for sid in schedule_ids:
        result = op_process_payment(sid)
        if result == PaymentStatus.paid:
            paid_count += 1
        else:
            missed_count += 1

    elapsed = time.perf_counter() - start
    print(f"  Completed : {len(schedule_ids):,} payments "
          f"(paid={paid_count:,}, missed={missed_count:,})")
    print(f"  Elapsed   : {elapsed:.2f} s")
    print(f"  Throughput: {len(schedule_ids) / elapsed:,.0f} ops/sec")


# ─── ISOLATION: concurrent wallet debits ─────────────────────────────────────────

def isolation_test_concurrent_payments(n_threads: int = 10) -> None:
    """
    ISOLATION TEST — Proves that SELECT FOR UPDATE prevents lost-update anomalies
    when multiple threads race to debit the same wallet simultaneously.

    Scenario
    --------
    • Borrower wallet balance = $1 000
    • n_threads payment schedules, each for $200
      → only floor(1 000 / 200) = 5 can possibly succeed

    All threads start at the same instant (threading.Barrier) and call
    op_process_payment().  Each call opens its own transaction and issues
    SELECT FOR UPDATE on the PaymentSchedule row first (idempotency guard),
    then on both wallet rows (balance guard).

    Expected outcome WITH SELECT FOR UPDATE
    ---------------------------------------
    ✓  Exactly 5 instalments marked PAID
    ✓  Remaining 5 marked MISSED (balance exhausted)
    ✓  Final borrower balance = $0
    ✓  paid + missed == n_threads  (no lost updates, no phantom rows)
    ✓  No negative balance (CHECK constraint never violated)

    WITHOUT SELECT FOR UPDATE (the anomaly)
    ----------------------------------------
    Two threads both read $1 000, both decide "enough funds", both deduct $200
    and write $800 — so the lender is credited $400 while the borrower only
    loses $200.  Repeated across 10 threads this gives a wildly inconsistent
    final state.
    """
    WALLET_BALANCE = decimal.Decimal("1000")
    PAYMENT_AMOUNT = decimal.Decimal("200")
    max_payable    = int(WALLET_BALANCE / PAYMENT_AMOUNT)  # 5

    print(f"\n{'─'*60}")
    print(f"[ISOLATION] {n_threads} threads race to debit one ${WALLET_BALANCE} wallet")
    print(f"  Payment per thread : ${PAYMENT_AMOUNT}")
    print(f"  Max payable        : {max_payable}")
    print(f"{'─'*60}")

    # ── Setup ─────────────────────────────────────────────────────────────────────
    schedule_ids: List[uuid.UUID] = []
    bwallet_id:   uuid.UUID

    with get_session() as session:
        suffix   = uuid.uuid4().hex[:8]
        lender   = User(full_name="ISO Lender",
                        email=f"iso_lender_{suffix}@test.com",
                        password_hash="x", role=UserRole.lender)
        borrower = User(full_name="ISO Borrower",
                        email=f"iso_borrower_{suffix}@test.com",
                        password_hash="x", role=UserRole.borrower)
        session.add_all([lender, borrower])
        session.flush()

        l_wallet = Wallet(user_id=lender.user_id,
                          balance=decimal.Decimal("9999999"), currency="USD")
        b_wallet = Wallet(user_id=borrower.user_id,
                          balance=WALLET_BALANCE, currency="USD")
        session.add_all([l_wallet, b_wallet])
        session.flush()
        bwallet_id = b_wallet.wallet_id

        loan = GrantedLoan(
            borrower_id=borrower.user_id, lender_id=lender.user_id,
            principal_amount=WALLET_BALANCE,
            interest_rate=decimal.Decimal("5"),
            term_months=n_threads,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=30 * n_threads),
        )
        session.add(loan)
        session.flush()

        for i in range(n_threads):
            sched = PaymentSchedule(
                loan_id=loan.loan_id,
                installment_number=i + 1,
                due_date=date.today() + timedelta(days=30 * (i + 1)),
                amount_due=PAYMENT_AMOUNT,
                principal_portion=PAYMENT_AMOUNT * decimal.Decimal("0.9"),
                interest_portion=PAYMENT_AMOUNT  * decimal.Decimal("0.1"),
                status=PaymentStatus.scheduled,
                from_wallet_id=b_wallet.wallet_id,
                to_wallet_id=l_wallet.wallet_id,
            )
            session.add(sched)
            session.flush()
            schedule_ids.append(sched.schedule_id)

    # ── Concurrent execution ──────────────────────────────────────────────────────
    errors:  List[str] = []
    barrier = threading.Barrier(n_threads)  # all threads depart at the same instant

    def run(sid: uuid.UUID) -> None:
        try:
            barrier.wait()
            op_process_payment(sid)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run, args=(sid,)) for sid in schedule_ids]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t_start

    # ── Verification ──────────────────────────────────────────────────────────────
    with get_session() as session:
        rows  = session.query(PaymentSchedule).filter(
            PaymentSchedule.schedule_id.in_(schedule_ids)
        ).all()
        paid   = sum(1 for r in rows if r.status == PaymentStatus.paid)
        missed = sum(1 for r in rows if r.status == PaymentStatus.missed)
        final  = session.query(Wallet).filter_by(wallet_id=bwallet_id).one().balance

    accounted  = paid + missed == n_threads
    no_anomaly = paid <= max_payable and final >= 0

    print(f"  Elapsed        : {elapsed:.3f} s")
    print(f"  Paid           : {paid}   (expected ≤ {max_payable})")
    print(f"  Missed         : {missed}")
    print(f"  Final balance  : ${final}  (expected ≥ $0)")
    print(f"  Thread errors  : {len(errors)}")
    if errors:
        for e in errors[:3]:
            print(f"    ↳ {e}")
    print()
    if accounted and no_anomaly:
        print("  RESULT: PASS — no anomalies. SELECT FOR UPDATE works correctly.")
    else:
        print("  RESULT: FAIL — isolation violation detected!")
        if paid > max_payable:
            print(f"    ↳ {paid} payments succeeded; only {max_payable} should "
                  "(lost-update anomaly).")
        if final < 0:
            print(f"    ↳ Negative balance ${final} — CHECK constraint bypassed.")
        if not accounted:
            print(f"    ↳ paid({paid}) + missed({missed}) ≠ {n_threads} — rows missing.")


# ─── Demo entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  TipyToTipy — P2P Lending Platform")
    print("=" * 60)

    # 1. Schema
    print("\n[1/6] Creating tables...")
    create_tables()

    # 2. Sample data
    print("\n[2/6] Populating sample data...")
    populate_sample_data()

    # 3. Walk through all five business operations
    print("\n[3/6] Business operations demo...")

    lender_id = op_create_user(
        "Frank Funds", "frank@demo.com", "$2b$12$frank",
        UserRole.lender, decimal.Decimal("25000"),
    )
    print(f"  OP-1 create_user    → user_id={lender_id}")

    offer_id = op_post_lending_offer(
        lender_id, decimal.Decimal("10000"), decimal.Decimal("9.0"),
        6, 24, "personal,medical", 600,
    )
    print(f"  OP-2 post_offer     → offer_id={offer_id}")

    borrower_id = op_create_user(
        "Grace Grants", "grace@demo.com", "$2b$12$grace",
        UserRole.borrower, decimal.Decimal("500"),
    )
    with get_session() as session:
        req = LoanRequest(
            borrower_id=borrower_id, amount=decimal.Decimal("2000"),
            purpose="personal", term_months=12,
            max_interest_rate=decimal.Decimal("11.0"),
        )
        session.add(req)
        session.flush()
        req_id = req.request_id

    match_id = op_apply_to_offer(req_id, offer_id, InitiatedBy.borrower)
    print(f"  OP-3 apply_to_offer → match_id={match_id}")

    loan_id = op_approve_match(match_id, "lender")
    print(f"  OP-4 approve_match  → loan_id={loan_id}")

    with get_session() as session:
        first_sched = (
            session.query(PaymentSchedule)
            .filter_by(loan_id=loan_id)
            .order_by(PaymentSchedule.installment_number)
            .first()
        )
        sched_id = first_sched.schedule_id

    status = op_process_payment(sched_id)
    print(f"  OP-5 process_payment → status={status}")

    # 4. Performance — user creation
    print("\n[4/6] Performance test — user creation...")
    perf_test_create_users(n=500)

    # 5. Performance — payment processing
    print("\n[5/6] Performance test — payment processing...")
    perf_test_process_payments(n=500)

    # 6. Isolation
    print("\n[6/6] Isolation test — concurrent wallet debits...")
    isolation_test_concurrent_payments(n_threads=10)

    print("\n" + "=" * 60)
    print("  All done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
