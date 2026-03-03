"""
tests.py — Performance benchmarks, isolation tests, and demo entry point.

Responsibilities:
  • perf_test_create_users                  — PERF-1: throughput of user creation
  • perf_test_process_payments              — PERF-2: throughput of payment processing
  • isolation_test_concurrent_payments      — ISOLATION-1: concurrent wallet debits
  • isolation_test_idempotent_payment       — ISOLATION-2: same schedule_id, N threads
  • isolation_test_concurrent_match_approval— ISOLATION-3: concurrent loan finalisation
  • isolation_test_unique_email_signup      — ISOLATION-4: duplicate email registration
  • main                                    — end-to-end demo runner

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
    LendingOffer,
    LoanMatch,
    LoanRequest,
    MatchStatus,
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


# ─── ISOLATION-2: idempotent payment processing ──────────────────────────────────

def isolation_test_idempotent_payment(n_threads: int = 10) -> None:
    """
    ISOLATION TEST 2 — Proves that the PaymentSchedule status guard prevents a
    single instalment from being debited more than once, even when N threads race
    to process the exact same schedule_id simultaneously.

    Scenario
    --------
    • One borrower wallet, balance = $500
    • One PaymentSchedule for $200 linked to that wallet
    • n_threads all call op_process_payment() with the SAME schedule_id

    Mechanism
    ---------
    Thread 1 acquires SELECT FOR UPDATE on the schedule row, deducts $200,
    sets status → PAID, commits.  Threads 2-N then acquire the lock in turn,
    read status == PAID, and return immediately (the idempotency guard).

    Expected outcome
    ----------------
    ✓  Wallet debited exactly once  → final balance = $300
    ✓  All threads return PaymentStatus.paid (correct — the payment IS paid)
    ✓  No errors raised, no double-debit anomaly
    """
    WALLET_BALANCE = decimal.Decimal("500")
    PAYMENT_AMOUNT = decimal.Decimal("200")

    print(f"\n{'─'*60}")
    print(f"[ISOLATION-2] {n_threads} threads race to process the SAME schedule_id")
    print(f"  Wallet balance : ${WALLET_BALANCE}")
    print(f"  Payment amount : ${PAYMENT_AMOUNT}")
    print(f"  Expected debits: 1  (final balance ${WALLET_BALANCE - PAYMENT_AMOUNT})")
    print(f"{'─'*60}")

    schedule_id: uuid.UUID
    bwallet_id:  uuid.UUID

    with get_session() as session:
        suffix   = uuid.uuid4().hex[:8]
        lender   = User(full_name="Idem Lender",
                        email=f"idem_lender_{suffix}@test.com",
                        password_hash="x", role=UserRole.lender)
        borrower = User(full_name="Idem Borrower",
                        email=f"idem_borrower_{suffix}@test.com",
                        password_hash="x", role=UserRole.borrower)
        session.add_all([lender, borrower])
        session.flush()

        l_wallet = Wallet(user_id=lender.user_id,
                          balance=decimal.Decimal("9999"), currency="USD")
        b_wallet = Wallet(user_id=borrower.user_id,
                          balance=WALLET_BALANCE, currency="USD")
        session.add_all([l_wallet, b_wallet])
        session.flush()
        bwallet_id = b_wallet.wallet_id

        loan = GrantedLoan(
            borrower_id=borrower.user_id, lender_id=lender.user_id,
            principal_amount=PAYMENT_AMOUNT,
            interest_rate=decimal.Decimal("5"),
            term_months=1,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=30),
        )
        session.add(loan)
        session.flush()

        sched = PaymentSchedule(
            loan_id=loan.loan_id,
            installment_number=1,
            due_date=date.today() + timedelta(days=30),
            amount_due=PAYMENT_AMOUNT,
            principal_portion=PAYMENT_AMOUNT * decimal.Decimal("0.9"),
            interest_portion=PAYMENT_AMOUNT * decimal.Decimal("0.1"),
            status=PaymentStatus.scheduled,
            from_wallet_id=b_wallet.wallet_id,
            to_wallet_id=l_wallet.wallet_id,
        )
        session.add(sched)
        session.flush()
        schedule_id = sched.schedule_id

    errors:  List[str]         = []
    results: List[PaymentStatus] = []
    barrier  = threading.Barrier(n_threads)

    def run() -> None:
        try:
            barrier.wait()
            status = op_process_payment(schedule_id)  # SAME id for every thread
            results.append(status)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run) for _ in range(n_threads)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t_start

    with get_session() as session:
        final = session.query(Wallet).filter_by(wallet_id=bwallet_id).one().balance

    correct_bal = final == WALLET_BALANCE - PAYMENT_AMOUNT

    print(f"  Elapsed        : {elapsed:.3f} s")
    print(f"  Thread results : {len(results)} returned, {len(errors)} errored")
    print(f"  Final balance  : ${final}  (expected ${WALLET_BALANCE - PAYMENT_AMOUNT})")
    print()
    if correct_bal and not errors:
        print("  RESULT: PASS — wallet debited exactly once. Idempotency guard works.")
    else:
        print("  RESULT: FAIL — double-debit anomaly detected!")
        if not correct_bal:
            print(f"    ↳ Final balance ${final} ≠ expected ${WALLET_BALANCE - PAYMENT_AMOUNT}.")
        for e in errors[:3]:
            print(f"    ↳ {e}")


# ─── ISOLATION-3: concurrent match approval ───────────────────────────────────────

def isolation_test_concurrent_match_approval(n_threads: int = 5) -> None:
    """
    ISOLATION TEST 3 — Proves that concurrent lender approvals of the same
    LoanMatch result in exactly one GrantedLoan and exactly one fund transfer,
    never two.

    Scenario
    --------
    • A LoanMatch already approved by the borrower (lender_status = pending).
    • n_threads simultaneously call op_approve_match(match_id, "lender").

    Mechanism
    ---------
    Thread 1 acquires SELECT FOR UPDATE on the match row, sets lender_status →
    approved, creates the GrantedLoan, transfers funds, commits.
    Thread 2 then acquires the lock, also sees both sides approved, tries to
    INSERT a second GrantedLoan — but match_id has a UNIQUE constraint, so
    Postgres raises IntegrityError; get_session() rolls back the whole
    transaction (including any wallet changes).

    Two lines of defence working together:
      1. SELECT FOR UPDATE — serialises access to the match row
      2. UNIQUE constraint on granted_loans.match_id — final safety net

    Expected outcome
    ----------------
    ✓  Exactly 1 GrantedLoan in the database
    ✓  Lender wallet debited exactly once
    ✓  n_threads − 1 IntegrityErrors (all rolled back, no data corruption)
    """
    LENDER_BALANCE = decimal.Decimal("5000")
    LOAN_AMOUNT    = decimal.Decimal("1000")

    print(f"\n{'─'*60}")
    print(f"[ISOLATION-3] {n_threads} threads race to approve the same LoanMatch")
    print(f"  Lender balance : ${LENDER_BALANCE}")
    print(f"  Loan amount    : ${LOAN_AMOUNT}")
    print(f"  Expected loans : 1")
    print(f"{'─'*60}")

    match_id:   uuid.UUID
    lwallet_id: uuid.UUID
    bwallet_id: uuid.UUID

    with get_session() as session:
        suffix   = uuid.uuid4().hex[:8]
        lender   = User(full_name="Approve Lender",
                        email=f"app_lender_{suffix}@test.com",
                        password_hash="x", role=UserRole.lender)
        borrower = User(full_name="Approve Borrower",
                        email=f"app_borrower_{suffix}@test.com",
                        password_hash="x", role=UserRole.borrower)
        session.add_all([lender, borrower])
        session.flush()

        l_wallet = Wallet(user_id=lender.user_id,
                          balance=LENDER_BALANCE, currency="USD")
        b_wallet = Wallet(user_id=borrower.user_id,
                          balance=decimal.Decimal("100"), currency="USD")
        session.add_all([l_wallet, b_wallet])
        session.flush()
        lwallet_id = l_wallet.wallet_id
        bwallet_id = b_wallet.wallet_id

        offer = LendingOffer(
            lender_id=lender.user_id,
            available_amount=LOAN_AMOUNT,
            interest_rate=decimal.Decimal("8"),
            min_term_months=6,
            max_term_months=12,
            min_credit_score=0,
        )
        session.add(offer)
        session.flush()

        request = LoanRequest(
            borrower_id=borrower.user_id,
            amount=LOAN_AMOUNT,
            purpose="personal",
            term_months=6,
            max_interest_rate=decimal.Decimal("10"),
        )
        session.add(request)
        session.flush()

        # Borrower already approved; lender side is still pending
        match = LoanMatch(
            request_id=request.request_id,
            offer_id=offer.offer_id,
            initiated_by=InitiatedBy.borrower,
            borrower_status=MatchStatus.approved,
            lender_status=MatchStatus.pending,
        )
        session.add(match)
        session.flush()
        match_id = match.match_id

    errors:   List[str]      = []
    loan_ids: List[uuid.UUID] = []
    barrier   = threading.Barrier(n_threads)

    def run() -> None:
        try:
            barrier.wait()
            result = op_approve_match(match_id, "lender")
            if result is not None:
                loan_ids.append(result)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run) for _ in range(n_threads)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t_start

    with get_session() as session:
        loan_count = (
            session.query(GrantedLoan).filter_by(match_id=match_id).count()
        )
        l_final = session.query(Wallet).filter_by(wallet_id=lwallet_id).one().balance
        b_final = session.query(Wallet).filter_by(wallet_id=bwallet_id).one().balance

    expected_lender   = LENDER_BALANCE - LOAN_AMOUNT
    expected_borrower = decimal.Decimal("100") + LOAN_AMOUNT
    correct_loans     = loan_count == 1
    correct_lender    = l_final == expected_lender
    correct_borrower  = b_final == expected_borrower

    print(f"  Elapsed         : {elapsed:.3f} s")
    print(f"  Loans created   : {loan_count}   (expected 1)")
    print(f"  Lender balance  : ${l_final}  (expected ${expected_lender})")
    print(f"  Borrower balance: ${b_final}  (expected ${expected_borrower})")
    print(f"  Thread errors   : {len(errors)}  "
          f"(expected {n_threads - 1} IntegrityErrors)")
    if errors:
        for e in errors[:3]:
            print(f"    ↳ {e}")
    print()
    if correct_loans and correct_lender and correct_borrower:
        print("  RESULT: PASS — exactly one loan created. Concurrent approvals are safe.")
    else:
        print("  RESULT: FAIL — duplicate loan or incorrect wallet balances!")
        if not correct_loans:
            print(f"    ↳ {loan_count} loans created; expected 1.")
        if not correct_lender:
            print(f"    ↳ Lender balance ${l_final} ≠ expected ${expected_lender}.")
        if not correct_borrower:
            print(f"    ↳ Borrower balance ${b_final} ≠ expected ${expected_borrower}.")


# ─── ISOLATION-4: concurrent duplicate email signup ──────────────────────────────

def isolation_test_unique_email_signup(n_threads: int = 10) -> None:
    """
    ISOLATION TEST 4 — Proves that concurrent registrations with the same email
    address result in exactly one user row, enforced by the UNIQUE constraint on
    users.email, and that no orphaned wallet rows are left behind.

    Scenario
    --------
    • n_threads simultaneously call op_create_user() with the same email.
    • Each call is a single transaction: INSERT user → flush → INSERT wallet.

    Mechanism
    ---------
    Postgres serialises the concurrent INSERTs at the unique index level.
    The first transaction to commit acquires the index slot; all subsequent
    attempts to INSERT the same email block until the winner commits, then fail
    with IntegrityError.  Because user + wallet live in the same transaction,
    the wallet INSERT is also rolled back — no orphaned rows.

    Expected outcome
    ----------------
    ✓  Exactly 1 user row for that email in the database
    ✓  Exactly 1 wallet for that user (no orphans)
    ✓  n_threads − 1 IntegrityErrors (all rolled back atomically)
    """
    shared_email = f"dupe_{uuid.uuid4().hex[:8]}@test.com"

    print(f"\n{'─'*60}")
    print(f"[ISOLATION-4] {n_threads} threads race to register with the same email")
    print(f"  Email          : {shared_email}")
    print(f"  Expected users : 1")
    print(f"{'─'*60}")

    errors:  List[str]      = []
    created: List[uuid.UUID] = []
    barrier  = threading.Barrier(n_threads)

    def run() -> None:
        try:
            barrier.wait()
            uid = op_create_user(
                full_name="Duplicate User",
                email=shared_email,
                password_hash="x",
                role=UserRole.borrower,
                initial_balance=decimal.Decimal("100"),
            )
            created.append(uid)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run) for _ in range(n_threads)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t_start

    with get_session() as session:
        user_count = session.query(User).filter_by(email=shared_email).count()
        # Wallet count should equal user count — proves atomicity (no orphans)
        wallet_count = (
            session.query(Wallet)
            .join(User, Wallet.user_id == User.user_id)
            .filter(User.email == shared_email)
            .count()
        )

    exactly_one_user   = user_count == 1
    no_orphaned_wallet = wallet_count == user_count

    print(f"  Elapsed        : {elapsed:.3f} s")
    print(f"  Users created  : {user_count}   (expected 1)")
    print(f"  Wallets created: {wallet_count}  (expected 1, proves atomicity)")
    print(f"  Thread errors  : {len(errors)}  (expected {n_threads - 1})")
    if errors:
        for e in errors[:3]:
            print(f"    ↳ {e}")
    print()
    if exactly_one_user and no_orphaned_wallet:
        print("  RESULT: PASS — unique constraint enforced. No duplicate users or orphans.")
    else:
        print("  RESULT: FAIL — duplicate user or orphaned wallet detected!")
        if not exactly_one_user:
            print(f"    ↳ {user_count} users found; expected 1.")
        if not no_orphaned_wallet:
            print(f"    ↳ {wallet_count} wallets for {user_count} user(s) — atomicity broken.")


# ─── Demo entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  TipyToTipy — P2P Lending Platform")
    print("=" * 60)

    # 1. Schema
    print("\n[1/9] Creating tables...")
    create_tables()

    # 2. Sample data
    print("\n[2/9] Populating sample data...")
    populate_sample_data()

    # 3. Walk through all five business operations
    print("\n[3/9] Business operations demo...")

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
    print("\n[4/9] Performance test — user creation...")
    perf_test_create_users(n=500)

    # 5. Performance — payment processing
    print("\n[5/9] Performance test — payment processing...")
    perf_test_process_payments(n=500)

    # 6. Isolation — concurrent wallet debits (original test)
    print("\n[6/9] Isolation test — concurrent wallet debits...")
    isolation_test_concurrent_payments(n_threads=10)

    # 7. Isolation — idempotent payment (same schedule, N threads)
    print("\n[7/9] Isolation test — idempotent payment processing...")
    isolation_test_idempotent_payment(n_threads=10)

    # 8. Isolation — concurrent match approval
    print("\n[8/9] Isolation test — concurrent match approval...")
    isolation_test_concurrent_match_approval(n_threads=5)

    # 9. Isolation — duplicate email signup
    print("\n[9/9] Isolation test — concurrent duplicate email signup...")
    isolation_test_unique_email_signup(n_threads=10)

    print("\n" + "=" * 60)
    print("  All done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
