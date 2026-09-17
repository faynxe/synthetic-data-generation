
"""
Synthetic Data Generator — retail schema
Tables: retail.customers, retail.orders
S3 Bucket: mm-fsi-fix
S3 Prefix: lc/agentcore-e2e-test/
Output: CSV

Usage:
    Modify the CONFIGURATION section constants below to adjust behaviour.

    # Change scale
    SCALE_FACTOR = 10.0          # 1000 customers, ~3000 orders

    # Switch output format
    OUTPUT_FORMAT = "parquet"

    # Switch to incremental mode
    MODE = "incremental"
    INCREMENTAL_OUTPUT = "delta"  # or "merged"
    INSERT_PCT = 10.0
    UPDATE_PCT = 5.0
    DELETE_PCT = 2.0

FIXES (v2):
    - status assignment switched from stochastic rng.choice to deterministic
      exact-count + shuffle, guaranteeing ±0% deviation at any scale factor.
    - validate() tolerance tightened from ±12% to ±5% to match the business rule.
"""

import io
import numpy as np
import pandas as pd
from faker import Faker
import boto3

# =============================================================================
# CONFIGURATION
# =============================================================================
SEED               = 42
SCALE_FACTOR       = 1.0          # multiplier for root table counts only
S3_BUCKET          = "mm-fsi-fix"
S3_PREFIX          = "lc/agentcore-e2e-test/"
OUTPUT_FORMAT      = "csv"        # "csv" or "parquet"
MODE               = "full"       # "full" or "incremental"
INCREMENTAL_OUTPUT = "merged"     # "merged" or "delta"

# Incremental change percentages
INSERT_PCT = 10.0
UPDATE_PCT =  5.0
DELETE_PCT =  2.0

# Root table base counts (scaled by SCALE_FACTOR)
ROOT_ROW_COUNTS = {
    "retail.customers": 100,
}

# Cardinality: avg children per parent
AVG_CHILDREN = {
    ("retail.customers", "retail.orders"): 3.0,
}

# Date bounds
SIGNUP_DATE_MIN = "2024-01-01"
SIGNUP_DATE_MAX = "2025-12-31"
ORDER_DATE_MAX  = "2025-12-31"

# Amount distribution (log-normal clipped to [AMOUNT_MIN, AMOUNT_MAX])
AMOUNT_MIN   =   5.00
AMOUNT_MAX   = 500.00
AMOUNT_MU    =   3.5   # log-normal mean (log-space)
AMOUNT_SIGMA =   1.0   # log-normal sigma (log-space)

# Status distribution
STATUS_VALUES = ["ACTIVE",  "INACTIVE", "CLOSED"]
STATUS_PROBS  = [0.70,       0.20,       0.10]

# Status distribution tolerance for validation (business rule: ±5%)
STATUS_TOLERANCE = 0.05


# =============================================================================
# HELPERS
# =============================================================================

def deterministic_status(n: int, rng: np.random.Generator) -> np.ndarray:
    """
    Assign status values with exact counts derived from STATUS_PROBS, then
    shuffle.  Guarantees the realized distribution is within ±1 row of target
    at any n, satisfying the ±5% business rule even at n=100.

    The last bucket absorbs any rounding remainder so counts always sum to n.
    """
    counts = []
    remainder = n
    for p in STATUS_PROBS[:-1]:
        c = round(n * p)
        counts.append(c)
        remainder -= c
    counts.append(remainder)   # absorbs rounding

    arr = np.concatenate(
        [np.full(c, lbl) for c, lbl in zip(counts, STATUS_VALUES)]
    )
    rng.shuffle(arr)
    return arr


# =============================================================================
# TABLE GENERATION
# =============================================================================

def generate_customers(n: int, rng: np.random.Generator, seed: int) -> pd.DataFrame:
    """Generate retail.customers with n rows."""
    fake = Faker()
    fake.seed_instance(seed)

    signup_min      = pd.Timestamp(SIGNUP_DATE_MIN)
    signup_max      = pd.Timestamp(SIGNUP_DATE_MAX)
    date_range_days = (signup_max - signup_min).days

    signup_dates = signup_min + pd.to_timedelta(
        rng.integers(0, date_range_days + 1, size=n), unit="D"
    )

    # FIX: deterministic exact-count assignment instead of stochastic rng.choice
    statuses = deterministic_status(n, rng)

    df = pd.DataFrame({
        "customer_id": np.arange(1, n + 1),
        "full_name":   [fake.name()         for _ in range(n)],
        "email":       [fake.unique.email()  for _ in range(n)],
        "signup_date": signup_dates.date,
        "status":      statuses,
    })
    return df


def generate_orders(customers_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate retail.orders; child counts derived from avg_children cardinality."""
    avg_per_parent = AVG_CHILDREN[("retail.customers", "retail.orders")]
    n_customers    = len(customers_df)

    # Poisson-distributed order counts, at least 1 per customer
    order_counts = rng.poisson(lam=avg_per_parent, size=n_customers)
    order_counts = np.clip(order_counts, 1, None)
    n_orders     = int(order_counts.sum())

    # Expand customer FK and signup_dates
    cust_ids        = np.repeat(customers_df["customer_id"].values, order_counts)
    signup_repeated = pd.to_datetime(
        np.repeat(customers_df["signup_date"].values, order_counts)
    )

    # order_date = signup_date + uniform random delta [0, days_until_ORDER_DATE_MAX]
    max_deltas = (pd.Timestamp(ORDER_DATE_MAX) - signup_repeated).days.values
    max_deltas = np.clip(max_deltas, 0, None)
    deltas     = np.array([rng.integers(0, int(d) + 1) for d in max_deltas])
    order_dates = signup_repeated + pd.to_timedelta(deltas, unit="D")

    # Right-skewed amounts: log-normal clipped to [AMOUNT_MIN, AMOUNT_MAX]
    raw_amounts = rng.lognormal(mean=AMOUNT_MU, sigma=AMOUNT_SIGMA, size=n_orders)
    amounts     = np.clip(raw_amounts, AMOUNT_MIN, AMOUNT_MAX).round(2)

    df = pd.DataFrame({
        "order_id":    np.arange(1, n_orders + 1),
        "customer_id": cust_ids,
        "order_date":  order_dates.date,
        "amount":      amounts,
    })
    return df


# =============================================================================
# CONSTRAINT ENFORCEMENT
# =============================================================================

def enforce_constraints(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Post-generation constraint checks; raises on critical violations."""
    if table_name == "retail.customers":
        assert df["customer_id"].is_unique,            "customer_id not unique"
        assert df["email"].is_unique,                  "email not unique"
        assert df["full_name"].notna().all(),           "full_name has NULLs"
        assert df["email"].notna().all(),               "email has NULLs"
        assert df["signup_date"].notna().all(),         "signup_date has NULLs"
        assert df["status"].notna().all(),              "status has NULLs"
        assert df["status"].isin(STATUS_VALUES).all(),  "invalid status values"
        min_d = pd.Timestamp(SIGNUP_DATE_MIN).date()
        max_d = pd.Timestamp(SIGNUP_DATE_MAX).date()
        assert (df["signup_date"] >= min_d).all() and (df["signup_date"] <= max_d).all(), \
            "signup_date out of range"

    elif table_name == "retail.orders":
        assert df["order_id"].is_unique,        "order_id not unique"
        assert df["customer_id"].notna().all(),  "customer_id has NULLs"
        assert df["order_date"].notna().all(),   "order_date has NULLs"
        assert (df["amount"] >= AMOUNT_MIN).all() and (df["amount"] <= AMOUNT_MAX).all(), \
            "amount out of range"

    return df


# =============================================================================
# VALIDATION
# =============================================================================

def validate(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> None:
    """Print a validation summary and raise on any failure."""
    print("\n" + "=" * 52)
    print("VALIDATION SUMMARY")
    print("=" * 52)

    # Row counts
    n_cust = len(customers_df)
    n_ord  = len(orders_df)
    target_cust = int(ROOT_ROW_COUNTS["retail.customers"] * SCALE_FACTOR)
    target_ord  = int(target_cust * AVG_CHILDREN[("retail.customers", "retail.orders")])
    print(f"  retail.customers : {n_cust:,} rows  (target {target_cust:,})")
    print(f"  retail.orders    : {n_ord:,} rows  (target ~{target_ord:,})")

    # FK integrity
    orphans = (~orders_df["customer_id"].isin(customers_df["customer_id"])).sum()
    print(f"  Orphan orders    : {orphans}  {'✅' if orphans == 0 else '❌'}")
    assert orphans == 0, "FK integrity failed"

    # Temporal constraint
    merged = orders_df.merge(
        customers_df[["customer_id", "signup_date"]], on="customer_id"
    )
    violations = (
        pd.to_datetime(merged["order_date"]) < pd.to_datetime(merged["signup_date"])
    ).sum()
    print(f"  order_date < signup_date violations: {violations}  {'✅' if violations == 0 else '❌'}")
    assert violations == 0, "Temporal constraint failed"

    # Status distribution — FIX: tolerance tightened to ±5% (business rule)
    dist = customers_df["status"].value_counts(normalize=True)
    print(f"  Status distribution (tolerance ±{STATUS_TOLERANCE:.0%}):")
    for status, expected in zip(STATUS_VALUES, STATUS_PROBS):
        actual = dist.get(status, 0.0)
        diff   = abs(actual - expected)
        flag   = "✅" if diff <= STATUS_TOLERANCE else "❌"
        print(f"    {status:10s}: {actual:.1%}  (target {expected:.0%}, diff={diff:.1%}) {flag}")
        assert diff <= STATUS_TOLERANCE, \
            f"Status {status} distribution {actual:.1%} deviates {diff:.1%} from target {expected:.0%} (limit ±{STATUS_TOLERANCE:.0%})"

    # Amount stats
    print(f"  Amount  — min:{orders_df['amount'].min():.2f}  max:{orders_df['amount'].max():.2f}  "
          f"mean:{orders_df['amount'].mean():.2f}  median:{orders_df['amount'].median():.2f}")

    # Customer coverage
    cov = orders_df["customer_id"].nunique()
    print(f"  Customers with ≥1 order: {cov}/{n_cust}  {'✅' if cov == n_cust else '❌'}")

    # PK / UNIQUE
    assert customers_df["customer_id"].is_unique, "customers PK not unique"
    assert orders_df["order_id"].is_unique,        "orders PK not unique"
    assert customers_df["email"].is_unique,         "email uniqueness violated"
    print("  PK / UNIQUE constraints: ✅")

    print("=" * 52)
    print("All validations passed ✅\n")


# =============================================================================
# INCREMENTAL HELPERS
# =============================================================================

def load_existing(bucket: str, prefix: str, fmt: str) -> dict:
    """Load existing tables from S3."""
    s3 = boto3.client("s3")
    tables = {}
    for schema_table in ["retail.customers", "retail.orders"]:
        schema, table = schema_table.split(".")
        key = f"{prefix}{schema}/{table}.{fmt}"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            if fmt == "csv":
                tables[schema_table] = pd.read_csv(io.BytesIO(obj["Body"].read()))
            else:
                tables[schema_table] = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            print(f"  Loaded {schema_table}: {len(tables[schema_table]):,} rows")
        except Exception as e:
            print(f"  WARNING: could not load {key} — {e}")
    return tables


def apply_incremental(
    existing: dict, rng: np.random.Generator, seed: int
) -> tuple:
    """
    Apply incremental changes: deletes → updates → inserts.
    Returns (merged_tables, delta_tables).
    Processing order:  deletes  → children first, then parents
                       inserts  → parents first, then children
    """
    customers = existing["retail.customers"].copy()
    orders    = existing["retail.orders"].copy()

    deltas = {"retail.customers": [], "retail.orders": []}

    # ── DELETES (children first) ─────────────────────────────────────────────
    n_order_del   = max(1, int(len(orders) * DELETE_PCT / 100))
    del_order_idx = rng.choice(orders.index, size=n_order_del, replace=False)
    del_orders    = orders.loc[del_order_idx].copy()
    del_orders["_operation"] = "DELETE"
    deltas["retail.orders"].append(del_orders)
    orders = orders.drop(index=del_order_idx).reset_index(drop=True)

    # Only delete customers that no longer have FK dependents
    cust_with_orders = set(orders["customer_id"].unique())
    deletable_custs  = customers[~customers["customer_id"].isin(cust_with_orders)].index
    n_cust_del = min(max(1, int(len(customers) * DELETE_PCT / 100)), len(deletable_custs))
    if n_cust_del > 0 and len(deletable_custs) > 0:
        del_cust_idx  = rng.choice(deletable_custs, size=n_cust_del, replace=False)
        del_customers = customers.loc[del_cust_idx].copy()
        del_customers["_operation"] = "DELETE"
        deltas["retail.customers"].append(del_customers)
        customers = customers.drop(index=del_cust_idx).reset_index(drop=True)

    # ── UPDATES (non-PK, non-FK columns only) ────────────────────────────────
    fake = Faker()
    fake.seed_instance(seed + 1)

    n_cust_upd   = max(1, int(len(customers) * UPDATE_PCT / 100))
    upd_cust_idx = rng.choice(customers.index, size=n_cust_upd, replace=False)
    customers.loc[upd_cust_idx, "status"]    = deterministic_status(n_cust_upd, rng)
    customers.loc[upd_cust_idx, "full_name"] = [fake.name() for _ in range(n_cust_upd)]
    upd_customers = customers.loc[upd_cust_idx].copy()
    upd_customers["_operation"] = "UPDATE"
    deltas["retail.customers"].append(upd_customers)

    n_ord_upd   = max(1, int(len(orders) * UPDATE_PCT / 100))
    upd_ord_idx = rng.choice(orders.index, size=n_ord_upd, replace=False)
    raw = rng.lognormal(mean=AMOUNT_MU, sigma=AMOUNT_SIGMA, size=n_ord_upd)
    orders.loc[upd_ord_idx, "amount"] = np.clip(raw, AMOUNT_MIN, AMOUNT_MAX).round(2)
    upd_orders = orders.loc[upd_ord_idx].copy()
    upd_orders["_operation"] = "UPDATE"
    deltas["retail.orders"].append(upd_orders)

    # ── INSERTS (parents first) ───────────────────────────────────────────────
    n_cust_ins  = max(1, int(len(customers) * INSERT_PCT / 100))
    max_cust_id = int(customers["customer_id"].max())
    signup_min  = pd.Timestamp(SIGNUP_DATE_MIN)
    signup_max  = pd.Timestamp(SIGNUP_DATE_MAX)
    date_range  = (signup_max - signup_min).days
    new_signup  = signup_min + pd.to_timedelta(
        rng.integers(0, date_range + 1, size=n_cust_ins), unit="D"
    )
    new_customers = pd.DataFrame({
        "customer_id": np.arange(max_cust_id + 1, max_cust_id + 1 + n_cust_ins),
        "full_name":   [fake.name()         for _ in range(n_cust_ins)],
        "email":       [fake.unique.email()  for _ in range(n_cust_ins)],
        "signup_date": new_signup.date,
        "status":      deterministic_status(n_cust_ins, rng),
    })
    new_customers_delta = new_customers.copy()
    new_customers_delta["_operation"] = "INSERT"
    deltas["retail.customers"].append(new_customers_delta)
    customers = pd.concat([customers, new_customers], ignore_index=True)

    n_ord_ins   = max(1, int(len(orders) * INSERT_PCT / 100))
    max_ord_id  = int(orders["order_id"].max())
    sample_cust = customers.sample(n=n_ord_ins, replace=True,
                                   random_state=int(rng.integers(9999)))
    signup_rep  = pd.to_datetime(sample_cust["signup_date"].values)
    max_deltas  = (pd.Timestamp(ORDER_DATE_MAX) - signup_rep).days.values
    max_deltas  = np.clip(max_deltas, 0, None)
    deltas_arr  = np.array([rng.integers(0, int(d) + 1) for d in max_deltas])
    new_orders  = pd.DataFrame({
        "order_id":    np.arange(max_ord_id + 1, max_ord_id + 1 + n_ord_ins),
        "customer_id": sample_cust["customer_id"].values,
        "order_date":  (signup_rep + pd.to_timedelta(deltas_arr, unit="D")).date,
        "amount":      np.clip(
            rng.lognormal(AMOUNT_MU, AMOUNT_SIGMA, n_ord_ins), AMOUNT_MIN, AMOUNT_MAX
        ).round(2),
    })
    new_orders_delta = new_orders.copy()
    new_orders_delta["_operation"] = "INSERT"
    deltas["retail.orders"].append(new_orders_delta)
    orders = pd.concat([orders, new_orders], ignore_index=True)

    merged = {"retail.customers": customers, "retail.orders": orders}
    delta  = {t: pd.concat(dfs, ignore_index=True) for t, dfs in deltas.items()}
    return merged, delta


# =============================================================================
# S3 UPLOAD
# =============================================================================

def upload(dataframes: dict, bucket: str, prefix: str, fmt: str) -> None:
    """Write each DataFrame to s3://bucket/prefix/schema/table.{fmt}."""
    s3 = boto3.client("s3")
    for schema_table, df in dataframes.items():
        schema, table = schema_table.split(".")
        key    = f"{prefix}{schema}/{table}.{fmt}"
        buffer = io.BytesIO()
        if fmt == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"
        else:
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        buffer.seek(0)
        s3.put_object(
            Bucket=bucket, Key=key,
            Body=buffer.getvalue(), ContentType=content_type
        )
        print(f"  Uploaded s3://{bucket}/{key}  ({len(df):,} rows, {len(df.columns)} cols)")


# =============================================================================
# FULL GENERATION
# =============================================================================

def generate_full(rng: np.random.Generator, seed: int) -> dict:
    n_customers = int(ROOT_ROW_COUNTS["retail.customers"] * SCALE_FACTOR)

    print(f"Generating retail.customers ({n_customers:,} rows)…")
    customers_df = generate_customers(n_customers, rng, seed)
    enforce_constraints(customers_df, "retail.customers")

    avg = AVG_CHILDREN[("retail.customers", "retail.orders")]
    print(f"Generating retail.orders (avg {avg} per customer)…")
    orders_df = generate_orders(customers_df, rng)
    enforce_constraints(orders_df, "retail.orders")

    return {"retail.customers": customers_df, "retail.orders": orders_df}


# =============================================================================
# MAIN
# =============================================================================

def main():
    rng = np.random.default_rng(SEED)

    print(f"Mode: {MODE.upper()}  |  Format: {OUTPUT_FORMAT.upper()}  |  "
          f"Seed: {SEED}  |  Scale: {SCALE_FACTOR}x")
    print(f"Target S3: s3://{S3_BUCKET}/{S3_PREFIX}\n")

    if MODE == "full":
        dataframes = generate_full(rng, SEED)
        validate(dataframes["retail.customers"], dataframes["retail.orders"])
        print("Uploading to S3…")
        upload(dataframes, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)

    elif MODE == "incremental":
        print("Loading existing data from S3…")
        existing = load_existing(S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
        merged, delta = apply_incremental(existing, rng, SEED)
        validate(merged["retail.customers"], merged["retail.orders"])

        if INCREMENTAL_OUTPUT == "merged":
            print("Uploading merged (full) dataset…")
            upload(merged, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
        else:
            n_cc = len(delta["retail.customers"])
            n_oc = len(delta["retail.orders"])
            print(f"Uploading delta (INSERT/UPDATE/DELETE) — "
                  f"{n_cc} customer changes, {n_oc} order changes…")
            upload(delta, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
    else:
        raise ValueError(f"Unknown MODE: {MODE!r}")

    print("\nDone ✅")


if __name__ == "__main__":
    main()
