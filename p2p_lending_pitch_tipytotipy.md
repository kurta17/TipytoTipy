# Global Data-Intensive Application — Part 01
## TipyToTipy (P2P Lending Platform)

---

## 1. Team

| Name | Role |
|------|------|
| Irakli Kereleishvili | Full Stack & Data |
| Giorgi Kurtanidze | Full Stack & Data |

---

## 2. Business Idea — Elevator Pitch

**TipyToTipy** is a peer-to-peer lending platform that connects people who need loans directly with individual investors — cutting out the bank.

The marketplace works both ways. Borrowers can post loan requests with their desired amount, term, and purpose. But lenders can also post lending offers — specifying how much capital they have, what interest rate they want, and what loan types or durations they accept. Borrowers then apply to those offers, and lenders can approve or reject each application. Both sides receive smart recommendations: borrowers see the most suitable lending offers for their profile, and lenders are shown the most creditworthy borrowers that match their preferences.

The platform handles everything in between: assessing borrower risk using AI-powered credit scoring, managing money movement with strict transaction guarantees, and distributing repayments back to lenders every month.

What makes it interesting from a data perspective is the variety of information it must handle simultaneously — structured financial transactions that cannot fail, unstructured documents like uploaded pay stubs, real-time event streams for payments and defaults, vector-based similarity search for credit scoring and recommendations, and an analytical layer that tracks platform health and investor returns over time.

---

## 3. Lending Platform — Entity Relationship Diagram

```
┌─────────────┐                          ┌─────────────┐
│   BORROWER  │                          │   LENDER    │
└──────┬──────┘                          └──────┬──────┘
       │                                        │
       │ posts (1 to many)        posts (1 to many)
       ▼                                        ▼
┌──────────────┐               ┌────────────────────┐
│ LOAN REQUEST │               │   LENDING OFFER    │
└──────┬───────┘               └────────┬───────────┘
       │                                │
       │        applies to              │
       └──────────────┬─────────────────┘
                      ▼
             ┌─────────────────┐
             │   APPLICATION   │
             │ (approved /     │
             │  rejected)      │
             └────────┬────────┘
                      │ approved → becomes
                      ▼
             ┌─────────────────┐
             │      LOAN       │
             └────────┬────────┘
                      │ has (1 to 1)
                      ▼
             ┌─────────────────┐
             │  REPAYMENT PLAN │
             └────────┬────────┘
                      │ consists of (1 to many)
                      ▼
             ┌─────────────────┐
             │    PAYMENT      │
             └────────┬────────┘
                      │ deducted from (many to 1)
                      ▼
             ┌─────────────────┐
             │     WALLET      │
             └─────────────────┘

 BORROWER ──(owns)──► WALLET
 LENDER   ──(owns)──► WALLET
```

### Entities Explained

- **BORROWER** — a user who needs a loan. Has a credit profile and financial history.
- **LENDER** — a user who invests money and expects a return. Defines their own lending terms.
- **LOAN REQUEST** — posted by a Borrower. Specifies amount needed, purpose, and preferred term.
- **LENDING OFFER** — posted by a Lender. Specifies available capital, interest rate, acceptable loan types, and duration range.
- **APPLICATION** — created when a Borrower applies to a Lending Offer. The Lender reviews it and approves or rejects. Both sides receive AI-powered recommendations to find the best match before this step.
- **LOAN** — created when an Application is approved. The active financial agreement between a Borrower and a Lender.
- **REPAYMENT PLAN** — generated automatically when a Loan is created. Defines the installment schedule.
- **PAYMENT** — a single monthly installment. Can be Paid, Missed, or Defaulted.
- **WALLET** — holds the balance of a Borrower or Lender. Money moves in and out through Payments.

### How It All Connects

A **Lender** posts a **Lending Offer** with their terms. A **Borrower** (guided by smart recommendations) applies to the best matching offer — creating an **Application**. The Lender reviews the borrower's profile and approves or rejects it. If approved, a **Loan** is created. The Loan generates a **Repayment Plan** made up of monthly **Payments**. Each payment is deducted from the Borrower's **Wallet** and transferred to the Lender's **Wallet**.

Borrowers can also post open **Loan Requests** for Lenders to browse and fund directly — the marketplace works in both directions.

---
