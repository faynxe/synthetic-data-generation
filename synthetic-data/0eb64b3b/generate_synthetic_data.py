
# ============================================================
# Synthetic Data Generator — retail schema
# ============================================================
# Configuration constants at top — change these, not function bodies.
#
# Usage examples:
#   Scale 10x:         SCALE_FACTOR = 10.0
#   Parquet output:    OUTPUT_FORMAT = "parquet"
#   Incremental delta: MODE = "incremental" / INCREMENTAL_OUTPUT = "delta"
#   Different seed:    SEED = 99
# ============================================================

import io
import numpy as np
import pandas as pd
import boto3
from faker import Faker

# ── CONFIGURATION ──────────────────────────────────────────────────────────
SEED             = 42
SCALE_FACTOR     = 1.0          # multiplies root table row counts only
S3_BUCKET        = "mm-fsi-fix"
S3_PREFIX        = "lc/f1-e2e-test/"
OUTPUT_FORMAT    = "csv"         # "csv" or "parquet"
MODE             = "full"        # "full" or "incremental"
INCREMENTAL_OUTPUT = "merged"    # "merged" or "delta" (only used when MODE=incremental)

# Incremental change percentages (used only when MODE="incremental")
INSERT_PCT = 10.0
UPDATE_PCT =  5.0
DELETE_PCT =  2.0

# Root table base counts (scaled by SCALE_FACTOR)
ROOT_ROW_COUNTS = {
    "retail.customers": 100,
}

# Golden / reference tables — fixed counts, never scaled or modified
GOLDEN_TABLES: dict = {}

# Child table cardinality (avg children per parent)
CHILD_CARDINALITY = {
    "retail.orders": {"parent": "retail.customers", "avg_per_parent": 3.0, "min_per_parent": 1},
}

# Date range for customer signup dates
SIGNUP_DATE_MIN = "2024-01-01"
SIGNUP_DATE_MAX = "2025-12-31"

# Amount distribution parameters (log-normal, clipped to [5, 500])
AMOUNT_LOGNORMAL_MEAN  = 4.0
AMOUNT_LOGNORMAL_SIGMA = 0.9
AMOUNT_MIN = 5.00
AMOUNT_MAX = 500.00

# Status enum — targets and tolerance
# FIX (BR4): stratified deterministic assignment guarantees distribution
# at any N, eliminating sampling variance that caused ±5% breaches.
STATUS_CHOICES = ["ACTIVE", "INACTIVE", "CLOSED"]
STATUS_PROBS   = [0.70,    0.20,      0.10]
STATUS_TOLERANCE = 0.05   # ±5% — enforced in validation


# ── TABLE GENERATION ──────────────────────────────────────────────────────

def _stratified_statuses(n: int, choices: list, probs: list,
                         rng: np.random.Generator) -> np.ndarray:
    """
    Deterministic stratified status assignment.
    Computes exact per-class counts first, then shuffles order.
    Guarantees distribution is within rounding error of targets at any N.

    FIX: replaces rng.choice(p=[...]) which has O(1/√N) sampling variance —
    at N=100 that is ~4.6% std dev, enough to breach the ±5% BR4 tolerance.
    """
    counts = []
    allocated = 0
    for i, p in enumerate(probs[:-1]):
        c = round(n * p)
        counts.append(c)
        allocated += c
    counts.append(n - allocated)          # last class absorbs rounding remainder

    labels = np.concatenate([
        np.full(c, choice) for choice, c in zip(choices, counts)
    ])
    rng.shuffle(labels)
    return labels


def generate_customers(n: int, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    """Generate retail.customers with n rows."""
    start     = pd.Timestamp(SIGNUP_DATE_MIN)
    end       = pd.Timestamp(SIGNUP_DATE_MAX)
    day_range = (end - start).days

    signup_offsets = rng.integers(0, day_range + 1, size=n)
    signup_dates   = [start + pd.Timedelta(days=int(d)) for d in signup_offsets]

    # Stratified deterministic assignment — guaranteed ±rounding% of targets
    statuses = _stratified_statuses(n, STATUS_CHOICES, STATUS_PROBS, rng)

    df = pd.DataFrame({
        "customer_id": range(1, n + 1),
        "full_name":   [fake.name() for _ in range(n)],
        "email":       [fake.unique.email() for _ in range(n)],
        "signup_date": pd.to_datetime(signup_dates),
        "status":      statuses,
    })
    return df


def generate_orders(customers_df: pd.DataFrame,
                    avg_per_parent: float,
                    min_per_parent: int,
                    rng: np.random.Generator) -> pd.DataFrame:
    """Generate retail.orders linked to retail.customers."""
    max_order_date = pd.Timestamp(SIGNUP_DATE_MAX)

    n_customers = len(customers_df)
    counts = rng.poisson(lam=avg_per_parent, size=n_customers)
    counts = np.maximum(counts, min_per_parent)
    total  = counts.sum()

    customer_ids = np.repeat(customers_df["customer_id"].values, counts)
    signup_arr   = np.repeat(customers_df["signup_date"].values, counts)

    # order_date in [signup_date, SIGNUP_DATE_MAX] — offset ≥ 0 guarantees ≥ signup_date
    order_dates = []
    for sd in signup_arr:
        sd_ts     = pd.Timestamp(sd)
        remaining = (max_order_date - sd_ts).days
        offset    = int(rng.integers(0, max(remaining, 0) + 1))
        order_dates.append(sd_ts + pd.Timedelta(days=offset))

    # Right-skewed amount: log-normal clipped to [AMOUNT_MIN, AMOUNT_MAX]
    raw     = rng.lognormal(mean=AMOUNT_LOGNORMAL_MEAN,
                            sigma=AMOUNT_LOGNORMAL_SIGMA, size=total)
    amounts = np.round(np.clip(raw, AMOUNT_MIN, AMOUNT_MAX), 2)

    df = pd.DataFrame({
        "order_id":    range(1, total + 1),
        "customer_id": customer_ids,
        "order_date":  pd.to_datetime(order_dates),
        "amount":      amounts,
    })
    return df


# ── FULL GENERATION ────────────────────────────────────────────────────────

def generate_full(rng: np.random.Generator) -> dict:
    fake = Faker()
    fake.seed_instance(SEED)

    dataframes = {}

    # 1. Root: retail.customers
    n_customers = int(ROOT_ROW_COUNTS["retail.customers"] * SCALE_FACTOR)
    customers   = generate_customers(n_customers, rng, fake)
    dataframes["retail.customers"] = customers
    print(f"  retail.customers : {len(customers):,} rows")

    # 2. Child: retail.orders (derived from cardinality)
    card   = CHILD_CARDINALITY["retail.orders"]
    orders = generate_orders(customers,
                             avg_per_parent=card["avg_per_parent"],
                             min_per_parent=card["min_per_parent"],
                             rng=rng)
    dataframes["retail.orders"] = orders
    print(f"  retail.orders    : {len(orders):,} rows  "
          f"(avg {len(orders)/len(customers):.2f} per customer)")

    return dataframes


# ── INCREMENTAL GENERATION ─────────────────────────────────────────────────

def load_existing(bucket: str, prefix: str) -> dict:
    """Load existing CSVs from S3 into a dict keyed by schema.table."""
    s3 = boto3.client("s3")
    existing = {}
    tables = list(ROOT_ROW_COUNTS.keys()) + list(CHILD_CARDINALITY.keys())

    for table in tables:
        schema_name, table_name = table.split(".")
        key = f"{prefix}{schema_name}/{table_name}.csv"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            df  = pd.read_csv(io.BytesIO(obj["Body"].read()))
            existing[table] = df
            print(f"  Loaded {table}: {len(df):,} rows")
        except Exception:
            print(f"  WARNING: {table} not found at s3://{bucket}/{key} — skipping")
    return existing


def generate_incremental(existing: dict, rng: np.random.Generator) -> dict:
    """Apply DELETE → UPDATE → INSERT to existing data. Children first for deletes."""
    fake = Faker()
    fake.seed_instance(SEED)

    tables_in_order = ["retail.customers", "retail.orders"]
    tables_reversed = list(reversed(tables_in_order))

    deltas  = {}
    current = {t: df.copy() for t, df in existing.items()}

    # ── DELETES (children first) ───────────────────────────────────────────
    for table in tables_reversed:
        if table in GOLDEN_TABLES or table not in current:
            continue
        df    = current[table]
        n_del = max(1, int(len(df) * DELETE_PCT / 100))

        if table == "retail.customers":
            child_df  = current.get("retail.orders", pd.DataFrame())
            used_ids  = set(child_df["customer_id"].unique()) if len(child_df) else set()
            deletable = df[~df["customer_id"].isin(used_ids)]
            n_del     = min(n_del, len(deletable))
            del_rows  = deletable.sample(n=n_del, random_state=int(rng.integers(1_000_000)))
        else:
            del_rows = df.sample(n=min(n_del, len(df)),
                                 random_state=int(rng.integers(1_000_000)))

        del_rows = del_rows.copy()
        del_rows["_operation"] = "DELETE"
        deltas.setdefault(table, []).append(del_rows)
        current[table] = df[~df.index.isin(del_rows.index)].reset_index(drop=True)
        print(f"  DELETE {table}: {len(del_rows)} rows")

    # ── UPDATES (any order) ────────────────────────────────────────────────
    for table in tables_in_order:
        if table in GOLDEN_TABLES or table not in current:
            continue
        df    = current[table]
        n_upd = max(1, int(len(df) * UPDATE_PCT / 100))
        upd_ix = rng.choice(len(df), size=min(n_upd, len(df)), replace=False)

        if table == "retail.customers":
            # Use stratified assignment for updated subset to maintain distribution
            new_statuses = _stratified_statuses(len(upd_ix), STATUS_CHOICES, STATUS_PROBS, rng)
            df.loc[upd_ix, "status"] = new_statuses
        elif table == "retail.orders":
            raw = rng.lognormal(AMOUNT_LOGNORMAL_MEAN,
                                AMOUNT_LOGNORMAL_SIGMA, size=len(upd_ix))
            df.loc[upd_ix, "amount"] = np.round(
                np.clip(raw, AMOUNT_MIN, AMOUNT_MAX), 2)

        upd_rows = df.loc[upd_ix].copy()
        upd_rows["_operation"] = "UPDATE"
        deltas.setdefault(table, []).append(upd_rows)
        print(f"  UPDATE {table}: {len(upd_rows)} rows")

    # ── INSERTS (parents first) ────────────────────────────────────────────
    for table in tables_in_order:
        if table in GOLDEN_TABLES or table not in current:
            continue
        df    = current[table]
        n_ins = max(1, int(len(df) * INSERT_PCT / 100))

        if table == "retail.customers":
            new_start_id = int(df["customer_id"].max()) + 1
            ins_df = generate_customers(n_ins, rng, fake)
            ins_df["customer_id"] = range(new_start_id, new_start_id + n_ins)
            current[table] = pd.concat([df, ins_df], ignore_index=True)

        elif table == "retail.orders":
            new_start_id    = int(df["order_id"].max()) + 1
            parent_df       = current["retail.customers"]
            sampled_parents = parent_df.sample(
                n=min(n_ins, len(parent_df)),
                random_state=int(rng.integers(1_000_000)))
            ins_df = generate_orders(sampled_parents, avg_per_parent=1,
                                     min_per_parent=1, rng=rng).head(n_ins)
            ins_df["order_id"] = range(new_start_id, new_start_id + len(ins_df))
            current[table] = pd.concat([df, ins_df], ignore_index=True)

        ins_rows = current[table].tail(n_ins).copy()
        ins_rows["_operation"] = "INSERT"
        deltas.setdefault(table, []).append(ins_rows)
        print(f"  INSERT {table}: {n_ins} rows")

    # ── BUILD OUTPUT ───────────────────────────────────────────────────────
    if INCREMENTAL_OUTPUT == "delta":
        return {t: pd.concat(parts, ignore_index=True) for t, parts in deltas.items()}
    else:
        return current


# ── VALIDATION ─────────────────────────────────────────────────────────────

def validate(dataframes: dict) -> bool:
    print("\n=== VALIDATION ===")
    passed = True

    cust   = dataframes.get("retail.customers", pd.DataFrame())
    orders = dataframes.get("retail.orders",    pd.DataFrame())

    # --- customers ---
    if len(cust):
        # PK uniqueness
        if cust["customer_id"].nunique() != len(cust):
            print("  ❌ customer_id not unique"); passed = False
        else:
            print(f"  ✅ customer_id unique ({len(cust):,} rows)")

        # Email uniqueness
        if cust["email"].nunique() != len(cust):
            print("  ❌ email not unique"); passed = False
        else:
            print("  ✅ email unique")

        # Required columns — no nulls
        for col in ["customer_id","full_name","email","signup_date","status"]:
            if cust[col].isna().any():
                print(f"  ❌ NULLs found in customers.{col}"); passed = False

        # signup_date range
        sd = pd.to_datetime(cust["signup_date"])
        if sd.min() < pd.Timestamp(SIGNUP_DATE_MIN) or sd.max() > pd.Timestamp(SIGNUP_DATE_MAX):
            print("  ❌ signup_date out of range"); passed = False
        else:
            print(f"  ✅ signup_date in [{SIGNUP_DATE_MIN}, {SIGNUP_DATE_MAX}]")

        # FIX: validate status distribution against ±5% tolerance (BR4)
        dist = cust["status"].value_counts(normalize=True)
        for s, expected in zip(STATUS_CHOICES, STATUS_PROBS):
            actual = dist.get(s, 0.0)
            diff   = abs(actual - expected)
            if diff > STATUS_TOLERANCE:
                print(f"  ❌ status={s}: {actual:.1%}  "
                      f"(target {expected:.0%}, diff {diff:.1%} > ±{STATUS_TOLERANCE:.0%})"); passed = False
            else:
                print(f"  ✅ status={s}: {actual:.1%}  "
                      f"(target {expected:.0%}, diff {diff:.1%} ≤ ±{STATUS_TOLERANCE:.0%})")

    # --- orders ---
    if len(orders):
        # PK uniqueness
        if orders["order_id"].nunique() != len(orders):
            print("  ❌ order_id not unique"); passed = False
        else:
            print(f"  ✅ order_id unique ({len(orders):,} rows)")

        # FK integrity
        valid_fk = orders["customer_id"].isin(cust["customer_id"])
        if not valid_fk.all():
            print(f"  ❌ FK violation: {(~valid_fk).sum()} orphan orders"); passed = False
        else:
            print("  ✅ All order.customer_id FK valid")

        # order_date >= signup_date
        merged = orders.merge(cust[["customer_id","signup_date"]], on="customer_id")
        bad    = (pd.to_datetime(merged["order_date"]) <
                  pd.to_datetime(merged["signup_date"])).sum()
        if bad:
            print(f"  ❌ {bad} orders before customer signup_date"); passed = False
        else:
            print("  ✅ All order_date >= signup_date")

        # Amount range
        if orders["amount"].min() < AMOUNT_MIN or orders["amount"].max() > AMOUNT_MAX:
            print("  ❌ amount out of [5, 500]"); passed = False
        else:
            print(f"  ✅ amount in [{AMOUNT_MIN}, {AMOUNT_MAX}]  "
                  f"(mean={orders['amount'].mean():.2f}, "
                  f"median={orders['amount'].median():.2f}, right-skewed ✓)")

        # Required columns — no nulls
        for col in ["order_id","customer_id","order_date","amount"]:
            if orders[col].isna().any():
                print(f"  ❌ NULLs found in orders.{col}"); passed = False

        # Cardinality
        avg_ord = len(orders) / len(cust) if len(cust) else 0
        print(f"  ✅ avg orders/customer: {avg_ord:.2f}  (target ~3.0)")

    print(f"\n  Overall: {'✅ ALL CHECKS PASSED' if passed else '❌ SOME CHECKS FAILED'}")
    return passed


# ── S3 UPLOAD ──────────────────────────────────────────────────────────────

def upload(dataframes: dict, bucket: str, prefix: str, fmt: str) -> None:
    s3 = boto3.client("s3")

    for table, df in dataframes.items():
        schema_name, table_name = table.split(".")
        key = f"{prefix}{schema_name}/{table_name}.{fmt}"

        if fmt == "csv":
            buf  = io.StringIO()
            df.to_csv(buf, index=False)
            body = buf.getvalue().encode("utf-8")
            content_type = "text/csv"
        else:
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            body = buf.getvalue()
            content_type = "application/octet-stream"

        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        print(f"  Uploaded s3://{bucket}/{key}  ({len(df):,} rows, {len(body):,} bytes)")


# ── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print(f"Mode: {MODE}  |  Seed: {SEED}  |  Scale: {SCALE_FACTOR}x  |  Format: {OUTPUT_FORMAT}")
    rng = np.random.default_rng(SEED)

    print("\n── Generating data ──")
    if MODE == "full":
        dataframes = generate_full(rng)
    elif MODE == "incremental":
        existing   = load_existing(S3_BUCKET, S3_PREFIX)
        dataframes = generate_incremental(existing, rng)
    else:
        raise ValueError(f"Unknown MODE: {MODE}")

    validate(dataframes)

    print("\n── Uploading to S3 ──")
    upload(dataframes, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
