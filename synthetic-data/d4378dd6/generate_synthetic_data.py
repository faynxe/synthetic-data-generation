
"""
Synthetic data generator for lc20_as / lc20_das / lc20_ps / lc20_uw_processing schemas.

Usage:
    python generate.py

To scale the dataset:
    SCALE_FACTOR = 10.0

To switch to incremental delta mode:
    MODE = "incremental"
    INCREMENTAL_OUTPUT = "delta"
    INSERT_PCT = 15.0

To switch to parquet:
    OUTPUT_FORMAT = "parquet"
"""
import io
import uuid
import numpy as np
import pandas as pd
from faker import Faker
import boto3

# ============================================================
# CONFIGURATION
# ============================================================
SEED = 42
SCALE_FACTOR = 1.0
S3_BUCKET = "mm-fsi-fix"
S3_PREFIX = "lc/runs/20261008-1729/"
OUTPUT_FORMAT = "csv"          # csv or parquet
MODE = "full"                  # full or incremental
INCREMENTAL_OUTPUT = "merged"  # merged or delta

INSERT_PCT = 10.0
UPDATE_PCT = 5.0
DELETE_PCT = 2.0

ROOT_ROW_COUNTS = {
    "lc20_as.as_application": 1000,
    "lc20_das.das_origination": 1000,
    "lc20_ps.ps_prescreen_entry": 1000,
    "lc20_uw_processing.uw_process": 1000,
}

GOLDEN_TABLES = {}  # none in this dataset

GENERATION_ORDER = [
    "lc20_as.as_application",
    "lc20_as.as_applicant",
    "lc20_as.as_applicant_detail",
    "lc20_as.as_application_certificate_of_d",
    "lc20_as.as_application_checking_account",
    "lc20_as.as_application_saving_account",
    "lc20_as.as_email_address",
    "lc20_das.das_origination",
    "lc20_das.das_member_origination",
    "lc20_das.das_origination_ext_ref",
    "lc20_ps.ps_prescreen_entry",
    "lc20_ps.ps_prescreen_ext_ref",
    "lc20_uw_processing.uw_process",
    "lc20_uw_processing.uw_process_run",
    "lc20_uw_processing.uw_process_run_ext_ref",
]

DATE_START = pd.Timestamp("2025-01-01")

# ============================================================
# HELPERS
# ============================================================

def hexid(rng, n, length=32):
    """Generate n unique hex ids truncated to `length` chars."""
    ints = rng.integers(0, 2**31, size=(n, 4), dtype=np.int64)
    out = []
    for row in ints:
        h = uuid.UUID(int=(int(row[0]) << 96) | (int(row[1]) << 64) | (int(row[2]) << 32) | int(row[3])).hex[:length]
        out.append(h)
    arr = np.array(out)
    if len(set(arr)) < n:
        arr = np.array(list(dict.fromkeys(arr)))
        seen = set(arr)
        while len(arr) < n:
            extra_row = rng.integers(0, 2**31, size=4, dtype=np.int64)
            h = uuid.UUID(int=(int(extra_row[0]) << 96) | (int(extra_row[1]) << 64) | (int(extra_row[2]) << 32) | int(extra_row[3])).hex[:length]
            if h not in seen:
                seen.add(h)
                arr = np.append(arr, h)
    return arr[:n]


def unique_timestamps(n, rng, start=DATE_START, span_days=400):
    """Generate n unique timestamps (date + time) within span_days of start.
    Using full datetime (not just date) guarantees uniqueness is always
    satisfiable regardless of how many rows are requested relative to the
    day-range, since each row also gets a distinct microsecond offset.
    """
    day_offsets = rng.integers(0, span_days, size=n)
    micro_offsets = rng.choice(86_400_000_000, size=n, replace=False)
    ts = start + pd.to_timedelta(day_offsets, unit="D") + pd.to_timedelta(micro_offsets, unit="us")
    ts = pd.Series(ts)
    while ts.duplicated().any():
        dup_idx = ts[ts.duplicated()].index
        ts.loc[dup_idx] = ts.loc[dup_idx] + pd.Timedelta(microseconds=1)
    return pd.DatetimeIndex(ts.values)


def after_timestamps(before, rng, min_delta_sec=1, max_delta_sec=864000):
    """Generate timestamps strictly after `before`, used for temporal_order
    constraints (e.g. modified_d >= created_d)."""
    deltas = rng.integers(min_delta_sec, max_delta_sec + 1, size=len(before))
    after = pd.DatetimeIndex(before) + pd.to_timedelta(deltas, unit="s")
    after = pd.Series(after)
    while after.duplicated().any():
        dup_idx = after[after.duplicated()].index
        after.loc[dup_idx] = after.loc[dup_idx] + pd.Timedelta(seconds=1)
    return pd.DatetimeIndex(after.values)


def enum_choice(rng, n, values, freqs):
    values = list(values)
    freqs = np.array([freqs[v] for v in values])
    freqs = freqs / freqs.sum()
    return rng.choice(values, size=n, p=freqs)


def nullify(rng, arr, null_pct):
    arr = arr.astype(object)
    n = len(arr)
    k = int(round(n * null_pct))
    if k > 0:
        idx = rng.choice(n, size=k, replace=False)
        arr[idx] = None
    return arr


FAKE = Faker()


# ============================================================
# TABLE GENERATORS (FULL MODE)
# ============================================================

def gen_as_application(n, seed):
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    application_d = unique_timestamps(n, rng, span_days=500)
    version = enum_choice(rng, n, ["0", "1"], {"0": 0.532, "1": 0.468}).astype(int)
    product_type = np.array(["PERSONAL_LOAN"] * n, dtype=object)
    product_type = nullify(rng, product_type, 0.904)
    app_type_vals = ["JOINT_CERTIFICATE_DEPOSIT_ACCOUNT", "JOINT_SAVINGS_ACCOUNT",
                      "JOINT_CHECKING_ACCOUNT", "CERTIFICATE_DEPOSIT_ACCOUNT",
                      "SAVINGS_ACCOUNT", "CHECKING_ACCOUNT"]
    app_type_freq = {"JOINT_CERTIFICATE_DEPOSIT_ACCOUNT": 0.281, "JOINT_SAVINGS_ACCOUNT": 0.2456,
                      "JOINT_CHECKING_ACCOUNT": 0.2179, "CERTIFICATE_DEPOSIT_ACCOUNT": 0.1582,
                      "SAVINGS_ACCOUNT": 0.0531, "CHECKING_ACCOUNT": 0.0442}
    app_type = enum_choice(rng, n, app_type_vals, app_type_freq).astype(object)
    app_type = nullify(rng, app_type, 0.096)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "application_d": application_d,
        "product_type_cd": product_type,
        "application_type_cd": app_type,
    })
    return df


def gen_as_applicant(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    version = enum_choice(rng, n, ["0", "1"], {"0": 0.527, "1": 0.473}).astype(int)
    sms = enum_choice(rng, n, ["0", "1"], {"0": 0.506, "1": 0.494}).astype(int)
    app_ids = rng.permutation(parent_ids)[:n]
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "application_id": app_ids,
        "primary_applicant_flg": np.ones(n, dtype=int),
        "sms_marketing_opt_in_flg": sms,
        "marketing_attribution_cd": np.array([None] * n, dtype=object),
    })
    return df


def gen_as_applicant_detail(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in range(11)]
    ver_freq = {"0": 0.104, "5": 0.098, "6": 0.097, "4": 0.096, "1": 0.095, "3": 0.094,
                "10": 0.09, "9": 0.088, "8": 0.081, "2": 0.079, "7": 0.078}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    applicant_ids = rng.permutation(parent_ids)[:n]
    first_names = np.array([FAKE.first_name() for _ in range(n)], dtype=object)
    last_names = np.array([FAKE.last_name() for _ in range(n)], dtype=object)
    dob = pd.Timestamp("1934-12-07") + pd.to_timedelta(
        rng.integers(0, (pd.Timestamp("2007-11-17") - pd.Timestamp("1934-12-07")).days, size=n), unit="D")
    ssn_set = set()
    ssn_list = []
    while len(ssn_list) < n:
        cand = f"{rng.integers(100,999)}-{rng.integers(10,99)}-{rng.integers(1000,9999)}"
        if cand not in ssn_set:
            ssn_set.add(cand)
            ssn_list.append(cand)
    ssn = np.array(ssn_list)
    emp_vals = ["OTHER", "RETIRED", "UNEMPLOYED", "EMPLOYED", "SELFEMPLOYED", "DISABLED"]
    emp_freq = {"OTHER": 0.1826, "RETIRED": 0.1804, "UNEMPLOYED": 0.1771, "EMPLOYED": 0.1648,
                "SELFEMPLOYED": 0.1537, "DISABLED": 0.1414}
    employment = enum_choice(rng, n, emp_vals, emp_freq).astype(object)
    employment = nullify(rng, employment, 0.102)
    tenure_vals = [str(i) for i in range(1, 21)]
    tenure_freq = {"11": 0.06, "7": 0.057, "10": 0.056, "8": 0.055, "4": 0.055, "2": 0.055,
                   "3": 0.054, "9": 0.054, "18": 0.053, "14": 0.053, "13": 0.051, "17": 0.05,
                   "16": 0.047, "20": 0.046, "5": 0.045, "1": 0.044, "12": 0.044, "19": 0.041,
                   "6": 0.041, "15": 0.039}
    tenure = enum_choice(rng, n, tenure_vals, tenure_freq).astype(int)

    def unique_decimal(lo, hi):
        vals = rng.uniform(lo, hi, size=n)
        vals = np.round(vals, 5)
        seen = set()
        for i, v in enumerate(vals):
            while v in seen:
                v = v + 1e-5
            seen.add(v)
            vals[i] = v
        return vals

    annual_income = unique_decimal(13.5021, 9998.64691)
    annual_add_income = unique_decimal(6.20378, 9947.23003)
    annual_income_taxable = unique_decimal(1.21818, 9997.39902)
    annual_income_non_taxable = unique_decimal(7.48881, 9994.96678)
    annual_housing_expense = unique_decimal(23.61788, 9996.5062)
    annual_add_debt = unique_decimal(0.55862, 9982.90525)

    housing_vals = ["OTHER", "OWN", "NONE", "RENT", "ANY", "MORTGAGE"]
    housing_freq = {"OTHER": 0.1801, "OWN": 0.1723, "NONE": 0.1689, "RENT": 0.1633,
                     "ANY": 0.1588, "MORTGAGE": 0.1566}
    housing = enum_choice(rng, n, housing_vals, housing_freq).astype(object)
    housing = nullify(rng, housing, 0.106)

    fico_min_vals = ["3", "5", "2", "1", "4"]
    fico_min_freq = {"3": 0.221, "5": 0.211, "2": 0.201, "1": 0.192, "4": 0.175}
    fico_min = enum_choice(rng, n, fico_min_vals, fico_min_freq).astype(int)
    fico_max_vals = ["5", "3", "2", "1", "4"]
    fico_max_freq = {"5": 0.231, "3": 0.219, "2": 0.188, "1": 0.181, "4": 0.181}
    fico_max = enum_choice(rng, n, fico_max_vals, fico_max_freq).astype(int)

    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "applicant_id": applicant_ids,
        "source_cd": np.array(["APPLICANT"] * n),
        "first_name": first_names,
        "first_name_keyn": np.array(["application-store_first_name_fek"] * n),
        "first_name_keyv": np.ones(n, dtype=int),
        "last_name": last_names,
        "last_name_keyn": np.array(["application-store_first_name_fek"] * n),
        "last_name_keyv": np.ones(n, dtype=int),
        "honorific_cd": np.array([None] * n, dtype=object),
        "date_of_birth": dob.date,
        "date_of_birth_keyn": np.array(["application-store_first_name_fek"] * n),
        "date_of_birth_keyv": np.ones(n, dtype=int),
        "ssn": ssn,
        "ssn_keyn": np.array(["application-store_first_name_fek"] * n),
        "ssn_keyv": np.ones(n, dtype=int),
        "marital_status_cd": np.array([None] * n, dtype=object),
        "educational_qualification_cd": np.array([None] * n, dtype=object),
        "employment_status_cd": employment,
        "cumulative_job_tenure_months": tenure,
        "annual_income": annual_income,
        "annual_additional_income": annual_add_income,
        "annual_income_taxable": annual_income_taxable,
        "annual_income_non_taxable": annual_income_non_taxable,
        "housing_status_cd": housing,
        "annual_housing_expense": annual_housing_expense,
        "annual_additional_debt": annual_add_debt,
        "fico_range_cd": np.array([None] * n, dtype=object),
        "fico_min": fico_min,
        "fico_max": fico_max,
        "citizenship": np.array([None] * n, dtype=object),
    })
    return df


def gen_as_application_certificate_of_d(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = ["5", "4", "3", "1", "0", "2"]
    ver_freq = {"5": 0.185, "4": 0.173, "3": 0.171, "1": 0.167, "0": 0.162, "2": 0.142}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    app_ids = rng.permutation(parent_ids)[:n]
    initial_deposit = rng.integers(155, 99837, size=n)
    prod_vals = ["25", "21", "14", "28", "6", "22", "12", "23", "26"]
    prod_freq = {"25": 0.134, "21": 0.12, "14": 0.115, "28": 0.115, "6": 0.113, "22": 0.112,
                 "12": 0.103, "23": 0.099, "26": 0.089}
    product_code = enum_choice(rng, n, prod_vals, prod_freq).astype(int)
    apy = np.round(rng.uniform(3.4, 5.0, size=n), 2)
    term = rng.integers(6, 61, size=n)
    is_joint = enum_choice(rng, n, ["0", "1"], {"0": 0.528, "1": 0.472}).astype(int)
    df = pd.DataFrame({
        "id": hexid(rng, n, length=18),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "source_cd": np.array(["APPLICANT"] * n),
        "initial_deposit_amount": initial_deposit,
        "product_code": product_code,
        "application_id": app_ids,
        "apy": apy,
        "term_in_months": term,
        "is_joint_flg": is_joint,
        "account_type": np.array(["CD"] * n),
    })
    return df


def gen_as_application_checking_account(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in [1, 2, 9, 4, 0, 3, 8, 6, 10, 5, 7]]
    ver_freq = {"1": 0.102, "2": 0.101, "9": 0.095, "4": 0.094, "0": 0.093, "3": 0.09,
                "8": 0.089, "6": 0.089, "10": 0.087, "5": 0.086, "7": 0.074}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    app_ids = rng.permutation(parent_ids)[:n]
    debit = enum_choice(rng, n, ["0", "1"], {"0": 0.527, "1": 0.473}).astype(int)
    min_dep = rng.integers(1, 1000, size=n)
    dep = rng.integers(3, 999, size=n)
    product_code = enum_choice(rng, n, ["1", "53"], {"1": 0.506, "53": 0.494}).astype(int)
    is_joint = enum_choice(rng, n, ["0", "1"], {"0": 0.506, "1": 0.494}).astype(int)
    df = pd.DataFrame({
        "id": hexid(rng, n, length=18),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "application_id": app_ids,
        "source_cd": np.array(["APPLICANT"] * n),
        "debit_card_requested_flg": debit,
        "min_initial_deposit_amount": min_dep,
        "initial_deposit_amount": dep,
        "product_code": product_code,
        "is_joint_flg": is_joint,
        "account_type": np.array(["ND"] * n),
    })
    return df


def gen_as_application_saving_account(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in [1, 3, 8, 10, 6, 2, 7, 5, 9, 4]]
    ver_freq = {"1": 0.115, "3": 0.106, "8": 0.102, "10": 0.101, "6": 0.101, "2": 0.097,
                "7": 0.097, "5": 0.095, "9": 0.094, "4": 0.092}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    app_ids = rng.permutation(parent_ids)[:n]
    dep = rng.integers(19, 9990, size=n)
    product_code = enum_choice(rng, n, ["25", "10", "9"], {"25": 0.362, "10": 0.322, "9": 0.316}).astype(int)
    is_joint = enum_choice(rng, n, ["0", "1"], {"0": 0.527, "1": 0.473}).astype(int)
    debit = enum_choice(rng, n, ["0", "1"], {"0": 0.506, "1": 0.494}).astype(int)
    df = pd.DataFrame({
        "id": hexid(rng, n, length=18),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "source_cd": np.array(["APPLICANT"] * n),
        "application_id": app_ids,
        "initial_deposit_amount": dep,
        "product_code": product_code,
        "is_joint_flg": is_joint,
        "debit_card_requested_flg": debit,
        "account_type": np.array(["SV"] * n),
    })
    return df


def gen_as_email_address(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in range(21)]
    ver_freq = {"20": 0.063, "12": 0.058, "14": 0.057, "3": 0.056, "4": 0.053, "15": 0.053,
                "6": 0.053, "5": 0.052, "17": 0.049, "7": 0.048, "18": 0.048, "9": 0.048,
                "1": 0.046, "10": 0.044, "19": 0.043, "8": 0.043, "11": 0.041, "2": 0.039,
                "16": 0.036, "13": 0.035, "0": 0.035}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    applicant_ids = rng.permutation(parent_ids)[:n]
    emails = [FAKE.unique.email() for _ in range(n)]
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "applicant_id": applicant_ids,
        "source_cd": np.array(["APPLICANT"] * n),
        "email_address": emails,
        "email_address_keyn": np.array(["application-store_email_address_fek"] * n),
        "email_address_keyv": np.ones(n, dtype=int),
    })
    return df


def gen_das_origination(n, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": np.zeros(n, dtype=int),
        "origination_guid": hexid(rng, n),
    })
    return df


def gen_das_member_origination(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    orig_ids = rng.permutation(parent_ids)[:n]
    member_ref_set = set()
    member_ref_list = []
    while len(member_ref_list) < n:
        cand = int(rng.integers(10836, 997603726))
        if cand not in member_ref_set:
            member_ref_set.add(cand)
            member_ref_list.append(cand)
    member_ref = np.array(member_ref_list)
    member_src = enum_choice(rng, n, ["1200", "1000"], {"1200": 0.508, "1000": 0.492}).astype(int)
    member_entity = enum_choice(rng, n, ["pfloginuserid", "loginactorid"],
                                 {"pfloginuserid": 0.505, "loginactorid": 0.495})
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": np.zeros(n, dtype=int),
        "origination_id": orig_ids,
        "member_ref_id": member_ref,
        "member_src_id": member_src,
        "member_entity_name": member_entity,
    })
    return df


def gen_das_origination_ext_ref(parent_ids, children_per_parent, seed):
    rng = np.random.default_rng(seed)
    origination_id = np.repeat(parent_ids, children_per_parent)
    n = len(origination_id)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in range(6)]
    ver_freq = {"0": 0.179, "1": 0.178, "5": 0.167, "4": 0.166, "3": 0.16, "2": 0.15}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    ext_src = enum_choice(rng, n, ["11004", "11000"], {"11004": 0.5, "11000": 0.5}).astype(int)
    ext_entity = enum_choice(rng, n, ["applicationid", "prescreenentry"],
                              {"applicationid": 0.5, "prescreenentry": 0.5})
    flow_vals = ["jointCertificateDepositAccountFlow", "checkingAccountFlow", "savingsAccountFlow",
                 "jointCheckingAccountFlow", "jointSavingsAccountFlow", "creditInABoxFlow",
                 "certificateDepositAccountFlow", "balanceTransferFlow"]
    flow_freq = {"jointCertificateDepositAccountFlow": 0.1446, "checkingAccountFlow": 0.1334,
                 "savingsAccountFlow": 0.1312, "jointCheckingAccountFlow": 0.1256,
                 "jointSavingsAccountFlow": 0.1211, "creditInABoxFlow": 0.1211,
                 "certificateDepositAccountFlow": 0.1177, "balanceTransferFlow": 0.1054}
    flow_id = enum_choice(rng, n, flow_vals, flow_freq).astype(object)
    flow_id = nullify(rng, flow_id, 0.108)
    fv_vals = ["0.0", "5.0", "4.0", "1.0", "3.0", "2.0"]
    fv_freq = {"0.0": 0.4096, "5.0": 0.1809, "4.0": 0.1143, "1.0": 0.106, "3.0": 0.1019, "2.0": 0.0873}
    flow_version = enum_choice(rng, n, fv_vals, fv_freq).astype(float).astype(object)
    flow_version = nullify(rng, flow_version, 0.519)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "origination_id": origination_id,
        "ext_ref_id": hexid(rng, n),
        "ext_src_id": ext_src,
        "ext_entity_name": ext_entity,
        "flow_id": flow_id,
        "flow_version": flow_version,
    })
    return df


def gen_ps_prescreen_entry(n, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    version = rng.integers(1, 863, size=n)
    src_vals = ["LIGHTBOX_AUTO", "EASYAPPLY", "PL_PRE_APPROVAL", "PL2AUTO", "PROSPECT", "PL2BT",
                "PL2PL_MB_TOPUP", "CLEANSWEEP", "PL2PL_CP_TOPUP", "JOINT_SAVINGS_ACCOUNT",
                "PL2PL_TOPUP", "PLDECLINE2AUTO", "EXPERIAN_CREDIT_MATCH", "LIGHTBOX"]
    src_freq = {"LIGHTBOX_AUTO": 0.0879, "EASYAPPLY": 0.0845, "PL_PRE_APPROVAL": 0.0793,
                "PL2AUTO": 0.0776, "PROSPECT": 0.0776, "PL2BT": 0.0759, "PL2PL_MB_TOPUP": 0.0724,
                "CLEANSWEEP": 0.069, "PL2PL_CP_TOPUP": 0.0672, "JOINT_SAVINGS_ACCOUNT": 0.0655,
                "PL2PL_TOPUP": 0.0638, "PLDECLINE2AUTO": 0.0621, "EXPERIAN_CREDIT_MATCH": 0.0586,
                "LIGHTBOX": 0.0586}
    source_cd = enum_choice(rng, n, src_vals, src_freq).astype(object)
    source_cd = nullify(rng, source_cd, 0.42)
    cp_vals = ["cleansweep", "PL", "personal"]
    cp_freq = {"cleansweep": 0.3365, "PL": 0.3365, "personal": 0.327}
    credit_policy_name = enum_choice(rng, n, cp_vals, cp_freq).astype(object)
    credit_policy_name = nullify(rng, credit_policy_name, 0.581)
    credit_policy_version = rng.integers(100, 345, size=n)
    effective_start = unique_timestamps(n, rng, span_days=500)
    first_look = unique_timestamps(n, rng, span_days=500)
    last_look = after_timestamps(first_look, rng)
    used_d = unique_timestamps(n, rng, span_days=500)
    offer_amt = np.round(rng.uniform(70.05, 99876.82, size=n), 2)
    seen = set()
    for i, v in enumerate(offer_amt):
        while v in seen:
            v = v + 0.01
        seen.add(v)
        offer_amt[i] = v
    purpose_vals = ["home_improvement", "credit_card", "wedding", "renewable_energy", "educational",
                    "debt_consolidation", "small_business", "major_purchase", "not_requested", "other",
                    "moving", "house", "vacation", "car", "medical"]
    purpose_freq = {"home_improvement": 0.0926, "credit_card": 0.0791, "wedding": 0.0774,
                     "renewable_energy": 0.0774, "educational": 0.0741, "debt_consolidation": 0.0707,
                     "small_business": 0.0673, "major_purchase": 0.0673, "not_requested": 0.0673,
                     "other": 0.0606, "moving": 0.0572, "house": 0.0572, "vacation": 0.0556,
                     "car": 0.0522, "medical": 0.0438}
    offer_purpose = enum_choice(rng, n, purpose_vals, purpose_freq).astype(object)
    offer_purpose = nullify(rng, offer_purpose, 0.406)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "source_cd": source_cd,
        "credit_policy_name": credit_policy_name,
        "credit_policy_version": credit_policy_version,
        "effective_start_d": effective_start,
        "effective_end_d": np.array([None] * n, dtype=object),
        "status_cd": np.array([None] * n, dtype=object),
        "first_look_up_d": first_look,
        "last_look_up_d": last_look,
        "used_d": used_d,
        "offer_amt": offer_amt,
        "offer_purpose_cd": offer_purpose,
    })
    return df


def gen_ps_prescreen_ext_ref(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    entry_ids = rng.permutation(parent_ids)[:n]
    src_vals = ["11004", "11001", "11002", "11008", "14000", "1000", "PL2BT"]
    src_freq = {"11004": 0.817, "11001": 0.053, "11002": 0.03, "11008": 0.027, "14000": 0.026,
                "1000": 0.024, "PL2BT": 0.023}
    ext_src = enum_choice(rng, n, src_vals, src_freq)
    ent_vals = ["applicationid", "actor", "loan", "referrer", "bpsrequestid", "program", "bankaccount",
                "accounttype", "product", "loanapp", "loginactorid", "cif", "productcode"]
    ent_freq = {"applicationid": 0.3, "actor": 0.241, "loan": 0.196, "referrer": 0.106,
                "bpsrequestid": 0.097, "program": 0.012, "bankaccount": 0.011, "accounttype": 0.008,
                "product": 0.007, "loanapp": 0.006, "loginactorid": 0.006, "cif": 0.006,
                "productcode": 0.004}
    ext_entity = enum_choice(rng, n, ent_vals, ent_freq)
    dir_cd = enum_choice(rng, n, ["SOURCE", "TARGET"], {"SOURCE": 0.8354, "TARGET": 0.1646}).astype(object)
    dir_cd = nullify(rng, dir_cd, 0.101)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": np.zeros(n, dtype=int),
        "ps_prescreen_entry_id": entry_ids,
        "ext_src_id": ext_src,
        "ext_ref_id": hexid(rng, n),
        "ext_entity_name": ext_entity,
        "ext_ref_direction_cd": dir_cd,
    })
    return df


def gen_uw_process(n, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in [2, 5, 9, 8, 10, 4, 6, 3, 7, 1]]
    ver_freq = {"2": 0.114, "5": 0.112, "9": 0.111, "8": 0.108, "10": 0.099, "4": 0.095,
                "6": 0.095, "3": 0.091, "7": 0.089, "1": 0.086}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    subj_entity = enum_choice(rng, n, ["actor", "applicationid", "address"],
                               {"actor": 0.359, "applicationid": 0.326, "address": 0.315})
    type_cd = enum_choice(rng, n, ["APPLICATION_DECISION", "ADDRESS_VALIDATION"],
                           {"APPLICATION_DECISION": 0.526, "ADDRESS_VALIDATION": 0.474}).astype(object)
    type_cd = nullify(rng, type_cd, 0.403)
    status_vals = ["SUCCESS", "NEW", "ACTIVE", "FAILURE", "EXPIRED", "WITHDRAWN", "CANCELED"]
    status_freq = {"SUCCESS": 0.413, "NEW": 0.111, "ACTIVE": 0.104, "FAILURE": 0.102,
                   "EXPIRED": 0.1, "WITHDRAWN": 0.092, "CANCELED": 0.078}
    status_cd = enum_choice(rng, n, status_vals, status_freq)
    status_d = unique_timestamps(n, rng, span_days=500)
    dec_status_vals = ["SUCCESS", "PENDING", "ERROR", "MORE_INFO", "FAILURE", "IN_PROGRESS"]
    dec_status_freq = {"SUCCESS": 0.443, "PENDING": 0.168, "ERROR": 0.102, "MORE_INFO": 0.101,
                        "FAILURE": 0.096, "IN_PROGRESS": 0.09}
    decision_status_cd = enum_choice(rng, n, dec_status_vals, dec_status_freq)
    decision_status_d = unique_timestamps(n, rng, span_days=500)
    dec_phase_vals = ["DRAW_VERIFICATION", "FULL_DECISION", "FULFILLMENT", "ADDRESS_VALIDATION",
                      "UNKNOWN", "BE", "FE", "BANK_ACCOUNT"]
    dec_phase_freq = {"DRAW_VERIFICATION": 0.1406, "FULL_DECISION": 0.1362, "FULFILLMENT": 0.1285,
                       "ADDRESS_VALIDATION": 0.1285, "UNKNOWN": 0.1218, "BE": 0.1218, "FE": 0.1196,
                       "BANK_ACCOUNT": 0.103}
    decision_phase_cd = enum_choice(rng, n, dec_phase_vals, dec_phase_freq).astype(object)
    decision_phase_cd = nullify(rng, decision_phase_cd, 0.097)
    subj_type_vals = ["JOINT_CERTIFICATE_DEPOSIT_ACCOUNT", "CERTIFICATE_DEPOSIT_ACCOUNT",
                      "JOINT_CHECKING_ACCOUNT", "SAVINGS_ACCOUNT", "JOINT_SAVINGS_ACCOUNT",
                      "CHECKING_ACCOUNT", "ADDRESS_BUSINESS", "ADDRESS", "CS_ORDER"]
    subj_type_freq = {"JOINT_CERTIFICATE_DEPOSIT_ACCOUNT": 0.236, "CERTIFICATE_DEPOSIT_ACCOUNT": 0.16,
                       "JOINT_CHECKING_ACCOUNT": 0.156, "SAVINGS_ACCOUNT": 0.111,
                       "JOINT_SAVINGS_ACCOUNT": 0.093, "CHECKING_ACCOUNT": 0.071,
                       "ADDRESS_BUSINESS": 0.064, "ADDRESS": 0.055, "CS_ORDER": 0.054}
    subject_type_cd = enum_choice(rng, n, subj_type_vals, subj_type_freq)
    expiration_d = unique_timestamps(n, rng, span_days=500)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "subject_ref_id": hexid(rng, n),
        "subject_entity_name": subj_entity,
        "type_cd": type_cd,
        "subject_src_id": hexid(rng, n),
        "status_cd": status_cd,
        "product_type_cd": np.array([None] * n, dtype=object),
        "status_d": status_d,
        "decision_status_cd": decision_status_cd,
        "decision_status_d": decision_status_d,
        "decision_phase_cd": decision_phase_cd,
        "subject_type_cd": subject_type_cd,
        "expiration_d": expiration_d,
    })
    return df


def gen_uw_process_run(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in [2, 1, 9, 4, 5, 6, 7, 3, 8, 10]]
    ver_freq = {"2": 0.112, "1": 0.109, "9": 0.109, "4": 0.103, "5": 0.102, "6": 0.097,
                "7": 0.097, "3": 0.095, "8": 0.089, "10": 0.087}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    uw_ids = rng.permutation(parent_ids)[:n]
    phase_vals = ["ADDRESS_VALIDATION", "FULFILLMENT", "FULL_DECISION", "BANK_ACCOUNT"]
    phase_freq = {"ADDRESS_VALIDATION": 0.257, "FULFILLMENT": 0.254, "FULL_DECISION": 0.247,
                  "BANK_ACCOUNT": 0.242}
    decision_phase_cd = enum_choice(rng, n, phase_vals, phase_freq)
    status_vals = ["PENDING", "MORE_INFO", "SUCCESS", "FAILURE", "IN_PROGRESS", "ERROR"]
    status_freq = {"PENDING": 0.191, "MORE_INFO": 0.169, "SUCCESS": 0.168, "FAILURE": 0.167,
                   "IN_PROGRESS": 0.159, "ERROR": 0.146}
    decision_status_cd = enum_choice(rng, n, status_vals, status_freq)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "uw_process_id": uw_ids,
        "decision_phase_cd": decision_phase_cd,
        "decision_status_cd": decision_status_cd,
    })
    return df


def gen_uw_process_run_ext_ref(n, parent_ids, seed):
    rng = np.random.default_rng(seed)
    created = unique_timestamps(n, rng)
    modified = after_timestamps(created, rng)
    ver_vals = [str(i) for i in [1, 4, 7, 3, 9, 8, 2, 10, 5, 6]]
    ver_freq = {"1": 0.129, "4": 0.11, "7": 0.107, "3": 0.106, "9": 0.101, "8": 0.1,
                "2": 0.094, "10": 0.092, "5": 0.081, "6": 0.08}
    version = enum_choice(rng, n, ver_vals, ver_freq).astype(int)
    run_ids = rng.permutation(parent_ids)[:n]
    ent_vals = ["journeyapplicationtoken", "loginactorid", "narmiuuid", "offerid", "entitytoken",
                "bankaccount", "applicantid", "prescreenentry", "cif"]
    ent_freq = {"journeyapplicationtoken": 0.122, "loginactorid": 0.119, "narmiuuid": 0.116,
                "offerid": 0.114, "entitytoken": 0.113, "bankaccount": 0.112, "applicantid": 0.103,
                "prescreenentry": 0.102, "cif": 0.099}
    ext_ref_entity_name = enum_choice(rng, n, ent_vals, ent_freq)
    df = pd.DataFrame({
        "id": hexid(rng, n),
        "created_d": created,
        "modified_d": modified,
        "version": version,
        "uw_process_run_id": run_ids,
        "ext_ref_id": hexid(rng, n),
        "ext_src_id": hexid(rng, n),
        "ext_ref_entity_name": ext_ref_entity_name,
    })
    return df


# ============================================================
# FULL GENERATION PIPELINE
# ============================================================

def generate_full(seed, scale_factor):
    tables = {}
    n_app = int(round(ROOT_ROW_COUNTS["lc20_as.as_application"] * scale_factor))
    n_orig = int(round(ROOT_ROW_COUNTS["lc20_das.das_origination"] * scale_factor))
    n_ps = int(round(ROOT_ROW_COUNTS["lc20_ps.ps_prescreen_entry"] * scale_factor))
    n_uw = int(round(ROOT_ROW_COUNTS["lc20_uw_processing.uw_process"] * scale_factor))

    s = seed
    tables["lc20_as.as_application"] = gen_as_application(n_app, s + 1)
    tables["lc20_as.as_applicant"] = gen_as_applicant(
        n_app, tables["lc20_as.as_application"]["id"].values, s + 2)
    tables["lc20_as.as_applicant_detail"] = gen_as_applicant_detail(
        n_app, tables["lc20_as.as_applicant"]["id"].values, s + 3)
    tables["lc20_as.as_application_certificate_of_d"] = gen_as_application_certificate_of_d(
        n_app, tables["lc20_as.as_application"]["id"].values, s + 4)
    tables["lc20_as.as_application_checking_account"] = gen_as_application_checking_account(
        n_app, tables["lc20_as.as_application"]["id"].values, s + 5)
    tables["lc20_as.as_application_saving_account"] = gen_as_application_saving_account(
        n_app, tables["lc20_as.as_application"]["id"].values, s + 6)
    tables["lc20_as.as_email_address"] = gen_as_email_address(
        n_app, tables["lc20_as.as_applicant"]["id"].values, s + 7)

    tables["lc20_das.das_origination"] = gen_das_origination(n_orig, s + 8)
    tables["lc20_das.das_member_origination"] = gen_das_member_origination(
        n_orig, tables["lc20_das.das_origination"]["id"].values, s + 9)
    tables["lc20_das.das_origination_ext_ref"] = gen_das_origination_ext_ref(
        tables["lc20_das.das_origination"]["id"].values, 2, s + 10)

    tables["lc20_ps.ps_prescreen_entry"] = gen_ps_prescreen_entry(n_ps, s + 11)
    tables["lc20_ps.ps_prescreen_ext_ref"] = gen_ps_prescreen_ext_ref(
        n_ps, tables["lc20_ps.ps_prescreen_entry"]["id"].values, s + 12)

    tables["lc20_uw_processing.uw_process"] = gen_uw_process(n_uw, s + 13)
    tables["lc20_uw_processing.uw_process_run"] = gen_uw_process_run(
        n_uw, tables["lc20_uw_processing.uw_process"]["id"].values, s + 14)
    tables["lc20_uw_processing.uw_process_run_ext_ref"] = gen_uw_process_run_ext_ref(
        n_uw, tables["lc20_uw_processing.uw_process_run"]["id"].values, s + 15)

    return tables


# ============================================================
# INCREMENTAL MODE
# ============================================================

PK_COLS = {
    "lc20_as.as_application": "id",
    "lc20_as.as_applicant": "id",
    "lc20_as.as_applicant_detail": "id",
    "lc20_as.as_application_certificate_of_d": "id",
    "lc20_as.as_application_checking_account": "id",
    "lc20_as.as_application_saving_account": "id",
    "lc20_as.as_email_address": "id",
    "lc20_das.das_origination": "id",
    "lc20_das.das_member_origination": "id",
    "lc20_das.das_origination_ext_ref": "id",
    "lc20_ps.ps_prescreen_entry": "id",
    "lc20_ps.ps_prescreen_ext_ref": "id",
    "lc20_uw_processing.uw_process": "id",
    "lc20_uw_processing.uw_process_run": "id",
    "lc20_uw_processing.uw_process_run_ext_ref": "id",
}

FK_COLS = {
    "lc20_as.as_applicant": ("application_id", "lc20_as.as_application"),
    "lc20_as.as_applicant_detail": ("applicant_id", "lc20_as.as_applicant"),
    "lc20_as.as_application_certificate_of_d": ("application_id", "lc20_as.as_application"),
    "lc20_as.as_application_checking_account": ("application_id", "lc20_as.as_application"),
    "lc20_as.as_application_saving_account": ("application_id", "lc20_as.as_application"),
    "lc20_as.as_email_address": ("applicant_id", "lc20_as.as_applicant"),
    "lc20_das.das_member_origination": ("origination_id", "lc20_das.das_origination"),
    "lc20_das.das_origination_ext_ref": ("origination_id", "lc20_das.das_origination"),
    "lc20_ps.ps_prescreen_ext_ref": ("ps_prescreen_entry_id", "lc20_ps.ps_prescreen_entry"),
    "lc20_uw_processing.uw_process_run": ("uw_process_id", "lc20_uw_processing.uw_process"),
    "lc20_uw_processing.uw_process_run_ext_ref": ("uw_process_run_id", "lc20_uw_processing.uw_process_run"),
}


def load_existing(bucket, prefix, fmt):
    s3 = boto3.client("s3")
    out = {}
    for table in GENERATION_ORDER:
        schema, name = table.split(".")
        key = f"{prefix}{schema}/{name}.{fmt}"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj["Body"].read()
            if fmt == "csv":
                out[table] = pd.read_csv(io.BytesIO(data))
            else:
                out[table] = pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            print(f"Could not load {key}: {e}")
            out[table] = pd.DataFrame()
    return out


NON_KEY_UPDATE_COLS = {
    "lc20_as.as_application": ["product_type_cd", "application_type_cd"],
    "lc20_as.as_applicant": ["sms_marketing_opt_in_flg"],
    "lc20_as.as_applicant_detail": ["employment_status_cd", "housing_status_cd", "fico_min", "fico_max"],
    "lc20_as.as_application_certificate_of_d": ["apy", "term_in_months"],
    "lc20_as.as_application_checking_account": ["debit_card_requested_flg"],
    "lc20_as.as_application_saving_account": ["debit_card_requested_flg"],
    "lc20_as.as_email_address": [],
    "lc20_das.das_origination": [],
    "lc20_das.das_member_origination": [],
    "lc20_das.das_origination_ext_ref": ["flow_id", "flow_version"],
    "lc20_ps.ps_prescreen_entry": ["offer_purpose_cd"],
    "lc20_ps.ps_prescreen_ext_ref": ["ext_ref_direction_cd"],
    "lc20_uw_processing.uw_process": ["status_cd", "decision_status_cd"],
    "lc20_uw_processing.uw_process_run": ["decision_status_cd"],
    "lc20_uw_processing.uw_process_run_ext_ref": [],
}


def generate_incremental(existing, rng, seed):
    changes = {t: {"insert": pd.DataFrame(), "update": pd.DataFrame(), "delete": pd.DataFrame()}
               for t in GENERATION_ORDER}
    current = {t: existing[t].copy() for t in GENERATION_ORDER}

    # --- DELETES (children first, i.e. reverse generation order) ---
    for table in reversed(GENERATION_ORDER):
        df = current[table]
        if df.empty:
            continue
        pk = PK_COLS[table]
        has_dependents = set()
        for child, (fk, parent) in FK_COLS.items():
            if parent == table and not current[child].empty:
                has_dependents |= set(current[child][fk].dropna().unique())
        eligible = df[~df[pk].isin(has_dependents)]
        n_delete = int(round(len(df) * DELETE_PCT / 100.0))
        n_delete = min(n_delete, len(eligible))
        if n_delete > 0:
            del_idx = rng.choice(eligible.index, size=n_delete, replace=False)
            deleted = df.loc[del_idx].copy()
            changes[table]["delete"] = deleted
            current[table] = df.drop(index=del_idx).reset_index(drop=True)

    # --- UPDATES ---
    for table in GENERATION_ORDER:
        df = current[table]
        if df.empty:
            continue
        cols = NON_KEY_UPDATE_COLS.get(table, [])
        n_update = int(round(len(df) * UPDATE_PCT / 100.0))
        if n_update > 0 and cols:
            upd_idx = rng.choice(df.index, size=n_update, replace=False)
            for col in cols:
                if col in df.columns:
                    vals = df[col].dropna().unique()
                    if len(vals) > 0:
                        df.loc[upd_idx, col] = rng.choice(vals, size=len(upd_idx))
            changes[table]["update"] = df.loc[upd_idx].copy()
        current[table] = df

    # --- INSERTS (parents first) ---
    gen_funcs_root = {
        "lc20_as.as_application": lambda n, s: gen_as_application(n, s),
        "lc20_das.das_origination": lambda n, s: gen_das_origination(n, s),
        "lc20_ps.ps_prescreen_entry": lambda n, s: gen_ps_prescreen_entry(n, s),
        "lc20_uw_processing.uw_process": lambda n, s: gen_uw_process(n, s),
    }
    gen_funcs_child = {
        "lc20_as.as_applicant": gen_as_applicant,
        "lc20_as.as_applicant_detail": gen_as_applicant_detail,
        "lc20_as.as_application_certificate_of_d": gen_as_application_certificate_of_d,
        "lc20_as.as_application_checking_account": gen_as_application_checking_account,
        "lc20_as.as_application_saving_account": gen_as_application_saving_account,
        "lc20_as.as_email_address": gen_as_email_address,
        "lc20_das.das_member_origination": gen_das_member_origination,
        "lc20_ps.ps_prescreen_ext_ref": gen_ps_prescreen_ext_ref,
        "lc20_uw_processing.uw_process_run": gen_uw_process_run,
        "lc20_uw_processing.uw_process_run_ext_ref": gen_uw_process_run_ext_ref,
    }

    seed_counter = seed + 1000
    for table in GENERATION_ORDER:
        df = current[table]
        base_n = max(len(df), 1)
        n_insert = int(round(base_n * INSERT_PCT / 100.0))
        if n_insert <= 0:
            continue
        seed_counter += 1
        if table in gen_funcs_root:
            new_rows = gen_funcs_root[table](n_insert, seed_counter)
        elif table == "lc20_das.das_origination_ext_ref":
            parent_ids = current["lc20_das.das_origination"][PK_COLS["lc20_das.das_origination"]].values
            sample_parents = rng.choice(parent_ids, size=max(1, n_insert // 2), replace=True)
            new_rows = gen_das_origination_ext_ref(sample_parents, 2, seed_counter)
        else:
            fk_col, parent_table = FK_COLS[table]
            parent_ids = current[parent_table][PK_COLS[parent_table]].values
            new_rows = gen_funcs_child[table](n_insert, parent_ids, seed_counter)
        changes[table]["insert"] = new_rows
        current[table] = pd.concat([df, new_rows], ignore_index=True)

    return current, changes


def build_delta_output(changes):
    delta = {}
    for table, parts in changes.items():
        frames = []
        if not parts["delete"].empty:
            d = parts["delete"].copy()
            d["_operation"] = "DELETE"
            frames.append(d)
        if not parts["update"].empty:
            u = parts["update"].copy()
            u["_operation"] = "UPDATE"
            frames.append(u)
        if not parts["insert"].empty:
            i = parts["insert"].copy()
            i["_operation"] = "INSERT"
            frames.append(i)
        if frames:
            delta[table] = pd.concat(frames, ignore_index=True)
        else:
            delta[table] = pd.DataFrame()
    return delta


# ============================================================
# VALIDATION
# ============================================================

def validate(tables):
    print("=== VALIDATION ===")
    all_ok = True
    for table, df in tables.items():
        pk = PK_COLS.get(table)
        if pk and pk in df.columns:
            dup = df[pk].duplicated().sum()
            nulls_pk = df[pk].isna().sum()
            if dup > 0 or nulls_pk > 0:
                all_ok = False
            print(f"{table}: {len(df)} rows | PK dup={dup} | nulls_pk={nulls_pk}")
        if "created_d" in df.columns and "modified_d" in df.columns:
            bad = (pd.to_datetime(df["modified_d"]) < pd.to_datetime(df["created_d"])).sum()
            if bad > 0:
                all_ok = False
            print(f"{table}: temporal_order violations (modified_d < created_d) = {bad}")
    for table, (fk, parent) in FK_COLS.items():
        if table in tables and parent in tables:
            child = tables[table]
            parent_df = tables[parent]
            if fk in child.columns:
                pk = PK_COLS[parent]
                valid = child[fk].isin(parent_df[pk]).all()
                if not valid:
                    all_ok = False
                print(f"FK check {table}.{fk} -> {parent}.{pk}: valid={valid}")
    print(f"=== END VALIDATION (all_ok={all_ok}) ===")
    return all_ok


# ============================================================
# S3 UPLOAD
# ============================================================

def upload(dataframes, bucket, prefix, fmt):
    s3 = boto3.client("s3")
    for table, df in dataframes.items():
        if df is None or df.empty:
            continue
        schema, name = table.split(".")
        key = f"{prefix}{schema}/{name}.{fmt}"
        buf = io.BytesIO()
        if fmt == "csv":
            df.to_csv(buf, index=False)
        else:
            df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        print(f"Uploaded s3://{bucket}/{key} ({len(df)} rows)")


# ============================================================
# MAIN
# ============================================================

def main():
    rng = np.random.default_rng(SEED)

    if MODE == "full":
        tables = generate_full(SEED, SCALE_FACTOR)
        validate(tables)
        upload(tables, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
    elif MODE == "incremental":
        existing = load_existing(S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
        merged, changes = generate_incremental(existing, rng, SEED)
        validate(merged)
        if INCREMENTAL_OUTPUT == "merged":
            upload(merged, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
        else:
            delta = build_delta_output(changes)
            upload(delta, S3_BUCKET, S3_PREFIX, OUTPUT_FORMAT)
    else:
        raise ValueError(f"Unknown MODE: {MODE}")


main()
