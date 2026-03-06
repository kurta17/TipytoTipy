"""
models.py — Database configuration, enums, ORM models, and schema DDL.

Responsibilities:
  • SQLAlchemy engine / session setup
  • Python Enum definitions (mirrored in PostgreSQL as ENUM types)
  • ORM model classes (one per ER entity)
  • Table creation / teardown helpers
"""

import decimal
import enum
import os
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

# ─── Connection ───────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/tipytotipy"
)

engine = create_engine(
    DATABASE_URL,
    echo=False,      # flip to True to log all emitted SQL
    pool_size=20,
    max_overflow=10,
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Yield a transactional database session.
    Commits on clean exit, rolls back on any exception, always closes.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ─── Enums ───────────────────────────────────────────────────────────────────────

class UserRole(str, enum.Enum):
    borrower = "borrower"
    lender   = "lender"
    both     = "both"


class LoanRequestStatus(str, enum.Enum):
    open    = "open"
    matched = "matched"
    closed  = "closed"


class LendingOfferStatus(str, enum.Enum):
    active        = "active"
    fully_matched = "fully_matched"
    closed        = "closed"


class InitiatedBy(str, enum.Enum):
    borrower = "borrower"
    lender   = "lender"


class MatchStatus(str, enum.Enum):
    pending  = "pending"
    approved = "approved"
    rejected = "rejected"


class LoanStatus(str, enum.Enum):
    active    = "active"
    completed = "completed"
    defaulted = "defaulted"


class PaymentStatus(str, enum.Enum):
    scheduled = "scheduled"
    paid      = "paid"
    missed    = "missed"
    defaulted = "defaulted"


# ─── ORM Models ──────────────────────────────────────────────────────────────────

class User(Base):
    """Platform user — can be a borrower, lender, or both."""
    __tablename__ = "users"

    user_id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    full_name     = Column(String(200), nullable=False)
    email         = Column(String(255), nullable=False)
    password_hash = Column(Text, nullable=False)
    role          = Column(Enum(UserRole, name="user_role"), nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow)

    wallet         = relationship("Wallet",       back_populates="user", uselist=False)
    credit_scores  = relationship("CreditScore",  back_populates="user")
    loan_requests  = relationship("LoanRequest",  back_populates="borrower",
                                  foreign_keys="LoanRequest.borrower_id")
    lending_offers = relationship("LendingOffer", back_populates="lender",
                                  foreign_keys="LendingOffer.lender_id")

    def __repr__(self) -> str:
        return f"<User {self.email} [{self.role}]>"


class Wallet(Base):
    """Holds a user's balance. Every user owns exactly one wallet."""
    __tablename__ = "wallets"
    __table_args__ = (
        CheckConstraint("balance >= 0", name="wallet_balance_non_negative"),
    )

    wallet_id  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("users.user_id"),
                        nullable=False, unique=True)
    balance    = Column(Numeric(15, 2), nullable=False, default=decimal.Decimal("0"))
    currency   = Column(String(3), nullable=False, default="USD")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="wallet")

    def __repr__(self) -> str:
        return f"<Wallet {self.balance} {self.currency}>"


class CreditScore(Base):
    """AI-generated credit score for a user (historical log)."""
    __tablename__ = "credit_scores"

    score_id      = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id       = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    score         = Column(SmallInteger, nullable=False)
    calculated_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="credit_scores")

    def __repr__(self) -> str:
        return f"<CreditScore {self.score} @ {self.calculated_at}>"


class LoanRequest(Base):
    """A borrower's open request for funding."""
    __tablename__ = "loan_requests"

    request_id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    borrower_id       = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    amount            = Column(Numeric(15, 2), nullable=False)
    purpose           = Column(Text, nullable=False)
    term_months       = Column(SmallInteger, nullable=False)
    max_interest_rate = Column(Numeric(5, 2), nullable=False)
    status            = Column(Enum(LoanRequestStatus, name="loan_request_status"),
                               nullable=False, default=LoanRequestStatus.open)
    document_url      = Column(Text)
    created_at        = Column(DateTime, default=datetime.utcnow)

    borrower     = relationship("User",      back_populates="loan_requests",
                                foreign_keys=[borrower_id])
    loan_matches = relationship("LoanMatch", back_populates="loan_request")

    def __repr__(self) -> str:
        return f"<LoanRequest ${self.amount} / {self.term_months}mo>"


class LendingOffer(Base):
    """A lender's offer of capital at specified terms."""
    __tablename__ = "lending_offers"

    offer_id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lender_id         = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    available_amount  = Column(Numeric(15, 2), nullable=False)
    interest_rate     = Column(Numeric(5, 2), nullable=False)
    min_term_months   = Column(SmallInteger, nullable=False)
    max_term_months   = Column(SmallInteger, nullable=False)
    accepted_purposes = Column(Text)
    min_credit_score  = Column(SmallInteger, nullable=False, default=0)
    status            = Column(Enum(LendingOfferStatus, name="lending_offer_status"),
                               nullable=False, default=LendingOfferStatus.active)
    created_at        = Column(DateTime, default=datetime.utcnow)

    lender       = relationship("User",      back_populates="lending_offers",
                                foreign_keys=[lender_id])
    loan_matches = relationship("LoanMatch", back_populates="lending_offer")

    def __repr__(self) -> str:
        return f"<LendingOffer ${self.available_amount} @ {self.interest_rate}%>"


class LoanMatch(Base):
    """
    Proposed pairing between a LoanRequest and a LendingOffer.
    Both sides must approve before a GrantedLoan is created.
    """
    __tablename__ = "loan_matches"
    __table_args__ = (
        UniqueConstraint("request_id", "offer_id", name="uq_loan_match_request_offer"),
    )

    match_id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    request_id      = Column(UUID(as_uuid=True), ForeignKey("loan_requests.request_id"),
                             nullable=False)
    offer_id        = Column(UUID(as_uuid=True), ForeignKey("lending_offers.offer_id"),
                             nullable=False)
    initiated_by    = Column(Enum(InitiatedBy, name="initiated_by"), nullable=False)
    borrower_status = Column(Enum(MatchStatus, name="borrower_match_status"),
                             nullable=False, default=MatchStatus.pending)
    lender_status   = Column(Enum(MatchStatus, name="lender_match_status"),
                             nullable=False, default=MatchStatus.pending)
    matched_at      = Column(DateTime)
    created_at      = Column(DateTime, default=datetime.utcnow)

    loan_request  = relationship("LoanRequest",  back_populates="loan_matches")
    lending_offer = relationship("LendingOffer",  back_populates="loan_matches")
    granted_loan  = relationship("GrantedLoan",   back_populates="loan_match", uselist=False)

    def __repr__(self) -> str:
        return f"<LoanMatch borrower={self.borrower_status} lender={self.lender_status}>"


class GrantedLoan(Base):
    """Active financial agreement once a LoanMatch is mutually approved."""
    __tablename__ = "granted_loans"

    loan_id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # nullable to allow direct creation in performance tests (bypassing full match flow)
    match_id         = Column(UUID(as_uuid=True), ForeignKey("loan_matches.match_id"),
                              nullable=True, unique=True)
    borrower_id      = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    lender_id        = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    principal_amount = Column(Numeric(15, 2), nullable=False)
    interest_rate    = Column(Numeric(5, 2), nullable=False)
    term_months      = Column(SmallInteger, nullable=False)
    start_date       = Column(Date, nullable=False)
    end_date         = Column(Date, nullable=False)
    status           = Column(Enum(LoanStatus, name="loan_status"),
                              nullable=False, default=LoanStatus.active)
    created_at       = Column(DateTime, default=datetime.utcnow)

    loan_match        = relationship("LoanMatch",       back_populates="granted_loan")
    payment_schedules = relationship("PaymentSchedule", back_populates="loan")

    def __repr__(self) -> str:
        return f"<GrantedLoan ${self.principal_amount} @ {self.interest_rate}% / {self.term_months}mo>"


class PaymentSchedule(Base):
    """
    Single monthly instalment in the amortized repayment schedule.
    Debited from the borrower's wallet and credited to the lender's wallet.
    """
    __tablename__ = "payment_schedules"

    schedule_id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    loan_id            = Column(UUID(as_uuid=True), ForeignKey("granted_loans.loan_id"),
                                nullable=False)
    installment_number = Column(SmallInteger, nullable=False)
    due_date           = Column(Date, nullable=False)
    amount_due         = Column(Numeric(15, 2), nullable=False)
    principal_portion  = Column(Numeric(15, 2), nullable=False)
    interest_portion   = Column(Numeric(15, 2), nullable=False)
    status             = Column(Enum(PaymentStatus, name="payment_status"),
                                nullable=False, default=PaymentStatus.scheduled)
    paid_at            = Column(DateTime)
    from_wallet_id     = Column(UUID(as_uuid=True), ForeignKey("wallets.wallet_id"),
                                nullable=False)
    to_wallet_id       = Column(UUID(as_uuid=True), ForeignKey("wallets.wallet_id"),
                                nullable=False)

    loan = relationship("GrantedLoan", back_populates="payment_schedules")

    def __repr__(self) -> str:
        return f"<PaymentSchedule #{self.installment_number} ${self.amount_due} [{self.status}]>"


# ─── Schema helpers ───────────────────────────────────────────────────────────────

def create_tables() -> None:
    """Emit CREATE TABLE IF NOT EXISTS DDL for every ORM-mapped table."""
    Base.metadata.create_all(bind=engine)
    print("Tables created:", ", ".join(Base.metadata.tables.keys()))


def drop_tables() -> None:
    """Drop all ORM-managed tables (useful for test teardown)."""
    Base.metadata.drop_all(bind=engine)
    print("All tables dropped.")
