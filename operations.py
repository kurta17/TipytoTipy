"""
operations.py — Sample data seeding and atomic business operations.

Responsibilities:
  • populate_sample_data  — seed the DB with a realistic starting dataset
  • _build_amortized_schedule — internal helper for generating payment schedules
  • op_create_user         — OP-1: create user + wallet atomically
  • op_post_lending_offer  — OP-2: lender publishes a new offer
  • op_apply_to_offer      — OP-3: borrower applies (or lender reaches out)
  • op_approve_match       — OP-4: mutual approval → loan + schedule created
  • op_process_payment     — OP-5: process one instalment atomically
"""

import decimal
import uuid
from datetime import date, datetime, timedelta
from typing import List, Optional

from sqlalchemy import text

from models import (
    CreditScore,
    GrantedLoan,
    InitiatedBy,
    LendingOffer,
    LendingOfferStatus,
    LoanMatch,
    LoanRequest,
    LoanRequestStatus,
    LoanStatus,
    MatchStatus,
    PaymentSchedule,
    PaymentStatus,
    User,
    UserRole,
    Wallet,
    get_session,
)

# ─── Sample Data ─────────────────────────────────────────────────────────────────

def populate_sample_data() -> None:
    """
    Insert a representative dataset covering every entity in the schema.

    Lifecycle demonstrated:
      users → wallets → credit scores → lending offers → loan requests
      → loan match (both approved) → granted loan → amortized payment schedule
    """
    with get_session() as session:
        # ── Clear existing data so re-runs don't hit unique-constraint errors ─────
        session.execute(text(
            "TRUNCATE users, wallets, credit_scores, loan_requests, "
            "lending_offers, loan_matches, granted_loans, payment_schedules "
            "RESTART IDENTITY CASCADE"
        ))
        session.flush()

        # ── Users ─────────────────────────────────────────────────────────────────
        alice   = User(full_name="Alice Investor",   email="alice@tipytotipy.io",
                       password_hash="$2b$12$alice_hash",   role=UserRole.lender)
        bob     = User(full_name="Bob Capital",      email="bob@tipytotipy.io",
                       password_hash="$2b$12$bob_hash",     role=UserRole.lender)
        charlie = User(full_name="Charlie Borrower", email="charlie@tipytotipy.io",
                       password_hash="$2b$12$charlie_hash", role=UserRole.borrower)
        diana   = User(full_name="Diana Needs",      email="diana@tipytotipy.io",
                       password_hash="$2b$12$diana_hash",   role=UserRole.borrower)
        eve     = User(full_name="Eve Startup",      email="eve@tipytotipy.io",
                       password_hash="$2b$12$eve_hash",     role=UserRole.both)
        session.add_all([alice, bob, charlie, diana, eve])
        session.flush()

        # ── Wallets ───────────────────────────────────────────────────────────────
        wallets = [
            Wallet(user_id=alice.user_id,   balance=decimal.Decimal("50000"), currency="USD"),
            Wallet(user_id=bob.user_id,     balance=decimal.Decimal("30000"), currency="USD"),
            Wallet(user_id=charlie.user_id, balance=decimal.Decimal("2000"),  currency="USD"),
            Wallet(user_id=diana.user_id,   balance=decimal.Decimal("1500"),  currency="USD"),
            Wallet(user_id=eve.user_id,     balance=decimal.Decimal("5000"),  currency="USD"),
        ]
        session.add_all(wallets)

        # ── Credit Scores ─────────────────────────────────────────────────────────
        session.add_all([
            CreditScore(user_id=charlie.user_id, score=720),
            CreditScore(user_id=diana.user_id,   score=650),
            CreditScore(user_id=eve.user_id,     score=700),
        ])

        # ── Lending Offers ────────────────────────────────────────────────────────
        offer_alice = LendingOffer(
            lender_id=alice.user_id, available_amount=decimal.Decimal("20000"),
            interest_rate=decimal.Decimal("8.50"), min_term_months=6, max_term_months=36,
            accepted_purposes="education,medical,home improvement", min_credit_score=600,
        )
        offer_bob = LendingOffer(
            lender_id=bob.user_id, available_amount=decimal.Decimal("15000"),
            interest_rate=decimal.Decimal("10.00"), min_term_months=3, max_term_months=24,
            accepted_purposes="business,personal", min_credit_score=550,
        )
        session.add_all([offer_alice, offer_bob])

        # ── Loan Requests ─────────────────────────────────────────────────────────
        req_charlie = LoanRequest(
            borrower_id=charlie.user_id, amount=decimal.Decimal("5000"),
            purpose="home improvement", term_months=12,
            max_interest_rate=decimal.Decimal("10.00"),
        )
        req_diana = LoanRequest(
            borrower_id=diana.user_id, amount=decimal.Decimal("3000"),
            purpose="medical", term_months=6,
            max_interest_rate=decimal.Decimal("12.00"),
        )
        req_eve = LoanRequest(
            borrower_id=eve.user_id, amount=decimal.Decimal("8000"),
            purpose="business", term_months=24,
            max_interest_rate=decimal.Decimal("11.00"),
        )
        session.add_all([req_charlie, req_diana, req_eve])
        session.flush()

        # ── Full lifecycle: charlie applies to alice's offer ───────────────────────
        match = LoanMatch(
            request_id=req_charlie.request_id, offer_id=offer_alice.offer_id,
            initiated_by=InitiatedBy.borrower,
            borrower_status=MatchStatus.approved,
            lender_status=MatchStatus.approved,
            matched_at=datetime.utcnow(),
        )
        session.add(match)
        session.flush()

        req_charlie.status = LoanRequestStatus.matched

        alice_wallet   = next(w for w in wallets if w.user_id == alice.user_id)
        charlie_wallet = next(w for w in wallets if w.user_id == charlie.user_id)
        alice_wallet.balance   -= req_charlie.amount
        charlie_wallet.balance += req_charlie.amount

        loan = GrantedLoan(
            match_id=match.match_id,
            borrower_id=charlie.user_id, lender_id=alice.user_id,
            principal_amount=req_charlie.amount, interest_rate=offer_alice.interest_rate,
            term_months=req_charlie.term_months,
            start_date=date.today(), end_date=date.today() + timedelta(days=365),
        )
        session.add(loan)
        session.flush()

        schedules = _build_amortized_schedule(
            loan_id=loan.loan_id,
            principal=float(req_charlie.amount),
            annual_rate=float(offer_alice.interest_rate),
            term_months=req_charlie.term_months,
            start_date=date.today(),
            from_wallet_id=charlie_wallet.wallet_id,
            to_wallet_id=alice_wallet.wallet_id,
        )
        session.add_all(schedules)

    print(
        "Sample data inserted: 5 users, 5 wallets, 3 credit scores, "
        "2 lending offers, 3 loan requests, 1 granted loan, "
        f"{len(schedules)} payment schedules."
    )


# ─── Internal helper ──────────────────────────────────────────────────────────────

def _build_amortized_schedule(
    loan_id: uuid.UUID,
    principal: float,
    annual_rate: float,
    term_months: int,
    start_date: date,
    from_wallet_id: uuid.UUID,
    to_wallet_id: uuid.UUID,
) -> List[PaymentSchedule]:
    """
    Compute a standard equal-instalment (amortized) repayment schedule.
    Returns unsaved PaymentSchedule ORM objects — the caller must add them
    to a session.
    """
    monthly_rate = annual_rate / 100 / 12
    if monthly_rate == 0:
        instalment = principal / term_months
    else:
        instalment = (
            principal
            * (monthly_rate * (1 + monthly_rate) ** term_months)
            / ((1 + monthly_rate) ** term_months - 1)
        )

    schedules: List[PaymentSchedule] = []
    remaining = principal
    for i in range(1, term_months + 1):
        interest_part  = remaining * monthly_rate
        principal_part = instalment - interest_part
        remaining     -= principal_part
        schedules.append(
            PaymentSchedule(
                loan_id=loan_id,
                installment_number=i,
                due_date=start_date + timedelta(days=30 * i),
                amount_due=decimal.Decimal(f"{instalment:.2f}"),
                principal_portion=decimal.Decimal(f"{principal_part:.2f}"),
                interest_portion=decimal.Decimal(f"{interest_part:.2f}"),
                status=PaymentStatus.scheduled,
                from_wallet_id=from_wallet_id,
                to_wallet_id=to_wallet_id,
            )
        )
    return schedules


# ─── Atomic Business Operations ───────────────────────────────────────────────────

def op_create_user(
    full_name: str,
    email: str,
    password_hash: str,
    role: UserRole,
    initial_balance: decimal.Decimal = decimal.Decimal("0"),
) -> uuid.UUID:
    """
    OP-1 — Register a new user and create their wallet in one transaction.

    Atomicity: if wallet insertion fails, the user row is rolled back.
    Returns the new user_id.
    """
    with get_session() as session:
        user = User(full_name=full_name, email=email,
                    password_hash=password_hash, role=role)
        session.add(user)
        session.flush()  # materialises user_id before wallet FK is needed

        session.add(Wallet(user_id=user.user_id, balance=initial_balance, currency="USD"))
        return user.user_id


def op_post_lending_offer(
    lender_id: uuid.UUID,
    available_amount: decimal.Decimal,
    interest_rate: decimal.Decimal,
    min_term: int,
    max_term: int,
    accepted_purposes: str,
    min_credit_score: int,
) -> uuid.UUID:
    """
    OP-2 — Lender publishes a new Lending Offer.

    Guards: wallet balance is locked (SELECT FOR UPDATE) and must cover the
    offered amount — prevents phantom offers backed by insufficient funds.
    Returns the new offer_id.
    """
    with get_session() as session:
        wallet = (
            session.query(Wallet)
            .filter_by(user_id=lender_id)
            .with_for_update()
            .one()
        )
        if wallet.balance < available_amount:
            raise ValueError(
                f"Lender wallet ${wallet.balance} < offered amount ${available_amount}."
            )

        offer = LendingOffer(
            lender_id=lender_id,
            available_amount=available_amount,
            interest_rate=interest_rate,
            min_term_months=min_term,
            max_term_months=max_term,
            accepted_purposes=accepted_purposes,
            min_credit_score=min_credit_score,
        )
        session.add(offer)
        session.flush()
        return offer.offer_id


def op_apply_to_offer(
    request_id: uuid.UUID,
    offer_id: uuid.UUID,
    initiated_by: InitiatedBy,
) -> uuid.UUID:
    """
    OP-3 — Create a LoanMatch between a LoanRequest and a LendingOffer.

    The initiating side is automatically marked approved; the other side is
    left as pending awaiting their approval (see op_approve_match).
    Duplicate applications are rejected (unique constraint on request+offer).
    Returns the new match_id.
    """
    with get_session() as session:
        if session.query(LoanMatch).filter_by(
            request_id=request_id, offer_id=offer_id
        ).first():
            raise ValueError(
                "A match between this loan request and lending offer already exists."
            )

        match = LoanMatch(
            request_id=request_id,
            offer_id=offer_id,
            initiated_by=initiated_by,
            borrower_status=(
                MatchStatus.approved if initiated_by == InitiatedBy.borrower
                else MatchStatus.pending
            ),
            lender_status=(
                MatchStatus.approved if initiated_by == InitiatedBy.lender
                else MatchStatus.pending
            ),
        )
        session.add(match)
        session.flush()
        return match.match_id


def op_approve_match(match_id: uuid.UUID, approver_role: str) -> Optional[uuid.UUID]:
    """
    OP-4 — Borrower or Lender approves a pending LoanMatch.

    When BOTH sides are approved in the same call:
      • Locks and transfers principal between wallets (SELECT FOR UPDATE).
      • Creates a GrantedLoan record.
      • Generates the full amortized PaymentSchedule.
      • Marks the LoanRequest as matched.

    Returns loan_id when a loan is created, None if still waiting for the
    other party.
    """
    if approver_role not in ("borrower", "lender"):
        raise ValueError("approver_role must be 'borrower' or 'lender'.")

    with get_session() as session:
        match   = session.query(LoanMatch).filter_by(match_id=match_id).with_for_update().one()
        request = session.query(LoanRequest).filter_by(request_id=match.request_id).one()
        offer   = session.query(LendingOffer).filter_by(offer_id=match.offer_id).one()

        if approver_role == "borrower":
            match.borrower_status = MatchStatus.approved
        else:
            match.lender_status = MatchStatus.approved

        if not (match.borrower_status == MatchStatus.approved
                and match.lender_status == MatchStatus.approved):
            return None  # still waiting for the other side

        # ── Both approved — fund the loan ──────────────────────────────────────
        match.matched_at = datetime.utcnow()
        request.status   = LoanRequestStatus.matched

        borrower_wallet = (
            session.query(Wallet).filter_by(user_id=request.borrower_id).with_for_update().one()
        )
        lender_wallet = (
            session.query(Wallet).filter_by(user_id=offer.lender_id).with_for_update().one()
        )
        if lender_wallet.balance < request.amount:
            raise ValueError(
                f"Lender wallet ${lender_wallet.balance} insufficient "
                f"to fund loan of ${request.amount}."
            )

        lender_wallet.balance   -= request.amount
        borrower_wallet.balance += request.amount

        start = date.today()
        loan  = GrantedLoan(
            match_id=match_id,
            borrower_id=request.borrower_id,
            lender_id=offer.lender_id,
            principal_amount=request.amount,
            interest_rate=offer.interest_rate,
            term_months=request.term_months,
            start_date=start,
            end_date=start + timedelta(days=30 * request.term_months),
        )
        session.add(loan)
        session.flush()

        session.add_all(
            _build_amortized_schedule(
                loan_id=loan.loan_id,
                principal=float(request.amount),
                annual_rate=float(offer.interest_rate),
                term_months=request.term_months,
                start_date=start,
                from_wallet_id=borrower_wallet.wallet_id,
                to_wallet_id=lender_wallet.wallet_id,
            )
        )
        return loan.loan_id


def op_process_payment(schedule_id: uuid.UUID) -> PaymentStatus:
    """
    OP-5 — Process a single scheduled payment instalment.

    Atomically (all within one transaction):
      1. Lock the PaymentSchedule row (SELECT FOR UPDATE) — idempotency guard.
      2. Lock both wallets (SELECT FOR UPDATE) — prevents lost-update anomaly.
      3. Deduct amount_due from borrower wallet; credit lender wallet.
      4. Mark instalment PAID, or MISSED if the borrower's balance is too low.

    Returns the final PaymentStatus of the instalment.
    """
    with get_session() as session:
        schedule = (
            session.query(PaymentSchedule)
            .filter_by(schedule_id=schedule_id)
            .with_for_update()
            .one()
        )
        if schedule.status != PaymentStatus.scheduled:
            return schedule.status  # already processed — idempotent

        from_wallet = (
            session.query(Wallet).filter_by(wallet_id=schedule.from_wallet_id).with_for_update().one()
        )
        to_wallet = (
            session.query(Wallet).filter_by(wallet_id=schedule.to_wallet_id).with_for_update().one()
        )

        if from_wallet.balance < schedule.amount_due:
            schedule.status = PaymentStatus.missed
            return PaymentStatus.missed

        from_wallet.balance -= schedule.amount_due
        to_wallet.balance   += schedule.amount_due
        schedule.status      = PaymentStatus.paid
        schedule.paid_at     = datetime.utcnow()
        return PaymentStatus.paid
