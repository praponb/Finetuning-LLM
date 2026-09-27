"""Shared table catalog for the SQL -> PySpark dataset.

Used by generate_dataset.py (to build queries), verify_pairs.py (to build small
seeded DataFrames and execute both sides) and eval/evaluate.py.

Tables flagged ``holdout=True`` are never used by the generator; they only
appear in the hand-written evaluation set so we can measure generalisation to
unseen table/column names.

EXTRA_TABLES (v4) hold nested data (structs, arrays of structs, maps, JSON strings)
and dense time series. Only the v4 families in advanced.py use them, so the v1-v3
families (and their outputs for a given seed) are unchanged.
"""
from __future__ import annotations

import random
import json
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any

# kind -> Spark SQL type
SPARK_TYPES = {
    "id": "int",
    "fk": "int",
    "int": "int",
    "double": "double",
    "cat": "string",
    "str": "string",
    "date": "date",
    "ts": "timestamp",
    "bool": "boolean",
    "array": "array<string>",
}

NUMERIC = {"id", "fk", "int", "double"}
STRING = {"cat", "str"}


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    pool: tuple = ()          # value pool for cat / str / array
    lo: float = 0             # numeric range
    hi: float = 100
    ref: str | None = None    # referenced table for fk columns
    nullable: bool = True
    dtype: str | None = None  # explicit Spark type for nested kinds (struct / array_struct / map / ...)

    @property
    def is_numeric(self) -> bool:
        return self.kind in NUMERIC

    @property
    def is_string(self) -> bool:
        return self.kind in STRING


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple
    holdout: bool = False
    rows: Any = None          # optional callable(seed) -> list[tuple] for hand-shaped sample data

    def col(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)

    @property
    def ddl(self) -> str:
        return ", ".join(f"{c.name} {c.dtype or SPARK_TYPES[c.kind]}" for c in self.columns)


def _id() -> Column:
    return Column("id", "id", lo=1, hi=40, nullable=False)


def _fk(name: str, ref: str, nullable: bool = True, hi: int = 40) -> Column:
    return Column(name, "fk", lo=1, hi=hi, ref=ref, nullable=nullable)


def _cat(name: str, *pool: str, nullable: bool = True) -> Column:
    return Column(name, "cat", pool=pool, nullable=nullable)


def _str(name: str, *pool: str) -> Column:
    return Column(name, "str", pool=pool)


def _num(name: str, lo: float, hi: float, kind: str = "double", nullable: bool = True) -> Column:
    return Column(name, kind, lo=lo, hi=hi, nullable=nullable)


def _date(name: str, nullable: bool = True) -> Column:
    return Column(name, "date", nullable=nullable)


def _ts(name: str) -> Column:
    return Column(name, "ts")


def _bool(name: str) -> Column:
    return Column(name, "bool")


PERSON_NAMES = (
    "Alice Wong", "bob smith", "  Carol Diaz", "David Lee ", "Emma Brown", "FRANK MILLER",
    "Grace Kim", "Henry Ford", "Ivy Chen", "Jack Taylor", "Kanya Srisuk", "Liam O'Neil",
    "Mia Rossi", "Noah Park", "Olivia Garcia", "Pim Chaiyaporn",
)
EMAILS = (
    "alice@gmail.com", "bob.smith@yahoo.com", "carol@company.co", "david.lee@gmail.com",
    "emma@outlook.com", "frank@company.co", "grace.kim@gmail.com", "henry@startup.io",
    "IVY@GMAIL.COM", "jack@startup.io", "kanya@mail.co.th", "liam@outlook.com",
)
COUNTRIES = ("TH", "US", "SG", "JP", "UK", "DE")
CITIES = ("Bangkok", "Chiang Mai", "New York", "Singapore", "Tokyo", "London", "Berlin")
REGIONS = ("North", "South", "East", "West", "Central")

TABLES: tuple[Table, ...] = (
    Table("customers", (
        _id(), _str("name", *PERSON_NAMES), _str("email", *EMAILS),
        _cat("country", *COUNTRIES, nullable=False), _cat("city", *CITIES),
        _cat("segment", "Consumer", "Corporate", "SMB"), _date("signup_date"),
        _bool("is_active"), _num("credit_limit", 500, 20000), _num("age", 18, 80, "int"),
    )),
    Table("orders", (
        _id(), _fk("customer_id", "customers"), _fk("store_id", "stores"), _date("order_date", nullable=False),
        _cat("status", "COMPLETED", "PENDING", "CANCELLED", "SHIPPED", "RETURNED", nullable=False),
        _cat("channel", "web", "mobile", "store", "partner"),
        _num("amount", 5, 2000), _num("discount", 0, 0.5), _num("shipping_fee", 0, 50),
    )),
    Table("order_items", (
        _id(), _fk("order_id", "orders", nullable=False), _fk("product_id", "products"),
        _num("quantity", 1, 10, "int"), _num("unit_price", 1, 500),
    )),
    Table("products", (
        _id(), _str("product_name", "Laptop Pro 14", "wireless mouse", "USB-C Hub", "Desk Lamp",
                    "Running Shoes", "Coffee Maker", "Yoga Mat", "  Water Bottle", "Noise-Cancel Headphones",
                    "Office Chair", "Backpack XL", "Smart Watch"),
        _cat("category", "Electronics", "Home", "Sports", "Books", "Fashion", "Grocery", nullable=False),
        _cat("brand", "Acme", "Globex", "Initech", "Umbrella", "Stark"),
        _num("price", 5, 1500), _num("cost", 2, 900), _num("stock_qty", 0, 500, "int"),
        _date("launch_date"),
        Column("tags", "array", pool=("sale", "new", "eco", "premium", "bestseller", "clearance")),
        _fk("supplier_id", "suppliers"),
    )),
    Table("suppliers", (
        _id(), _str("supplier_name", "Siam Parts Co", "Global Supply", "Tokyo Traders", "EuroGoods GmbH",
                    "Lion City Imports", "Pacific Wholesale"),
        _cat("country", *COUNTRIES, nullable=False), _cat("city", *CITIES), _num("rating", 1, 5),
    )),
    Table("employees", (
        _id(), _str("full_name", *PERSON_NAMES), _str("email", *EMAILS),
        _fk("dept_id", "departments", nullable=False), _fk("manager_id", "employees"),
        _cat("job_title", "Engineer", "Analyst", "Manager", "Director", "Intern", "Designer"),
        _num("salary", 25000, 180000, nullable=False), _date("hire_date"), _bool("is_remote"),
        _cat("level", "L1", "L2", "L3", "L4", "L5"),
    )),
    Table("departments", (
        _id(), _cat("dept_name", "Engineering", "Sales", "Marketing", "HR", "Finance", "Legal"),
        _cat("location", *CITIES), _num("budget", 100000, 5000000),
    )),
    Table("accounts", (
        _id(), _fk("customer_id", "customers"),
        _cat("account_type", "SAVINGS", "CHECKING", "CREDIT", "LOAN", nullable=False),
        _num("balance", -5000, 100000), _date("opened_date"), _cat("status", "OPEN", "CLOSED", "FROZEN"),
    )),
    Table("transactions", (
        _id(), _fk("account_id", "accounts", nullable=False),
        _cat("txn_type", "DEBIT", "CREDIT", "REFUND", "FEE", nullable=False),
        _num("amount", 1, 5000), _ts("txn_time"), _cat("merchant", "Grab", "7-Eleven", "Amazon", "Lazada",
                                                          "Starbucks", "Shell"),
        _bool("is_flagged"),
    )),
    Table("sessions", (
        _id(), _fk("customer_id", "customers"), _cat("device", "desktop", "mobile", "tablet", nullable=False),
        _cat("browser", "Chrome", "Safari", "Firefox", "Edge"), _ts("started_at"),
        _cat("country", *COUNTRIES),
    )),
    Table("web_events", (
        _id(), _fk("session_id", "sessions", nullable=False),
        _cat("event_type", "page_view", "click", "add_to_cart", "purchase", "search", nullable=False),
        _cat("page", "/home", "/product", "/cart", "/checkout", "/search"), _ts("event_time"),
        _num("duration_sec", 0, 600, "int"),
    )),
    Table("payments", (
        _id(), _fk("order_id", "orders"), _cat("method", "card", "paypal", "bank_transfer", "cash", "promptpay"),
        _num("amount", 5, 2000), _ts("paid_at"), _cat("status", "SUCCESS", "FAILED", "REFUNDED", nullable=False),
    )),
    Table("shipments", (
        _id(), _fk("order_id", "orders"), _cat("carrier", "DHL", "FedEx", "Kerry", "Flash", "UPS"),
        _date("shipped_date"), _date("delivered_date"), _num("cost", 2, 120),
        _cat("status", "IN_TRANSIT", "DELIVERED", "LOST", "RETURNED", nullable=False),
    )),
    Table("stores", (
        _id(), _str("store_name", "Siam Flagship", "Downtown Express", "Airport Kiosk", "Mall Central",
                    "Riverside", "Old Town"),
        _cat("city", *CITIES, nullable=False), _cat("region", *REGIONS), _date("opened_date"),
        _num("sqft", 500, 20000, "int"),
    )),
    Table("inventory", (
        _id(), _fk("store_id", "stores", nullable=False, hi=6), _fk("product_id", "products", nullable=False, hi=10),
        _num("quantity", 0, 300, "int"), _date("last_restock"),
    )),
    Table("store_sales", (
        _id(), _fk("store_id", "stores", nullable=False, hi=6), _fk("product_id", "products", nullable=False, hi=10),
        _date("sale_date"), _num("units", 1, 50, "int"), _num("revenue", 10, 5000),
    )),
    Table("subscriptions", (
        _id(), _fk("customer_id", "customers"), _cat("plan", "basic", "standard", "premium", "enterprise",
                                                      nullable=False),
        _date("start_date"), _date("end_date"), _num("monthly_fee", 5, 300),
        _cat("status", "ACTIVE", "CANCELLED", "PAUSED", "EXPIRED", nullable=False),
    )),
    # ---- hold-out tables: never used by the generator ----
    Table("patients", (
        _id(), _str("full_name", *PERSON_NAMES), _cat("gender", "F", "M", "X"), _date("birth_date"),
        _cat("city", *CITIES), _cat("insurance_type", "PUBLIC", "PRIVATE", "NONE", nullable=False),
        _bool("is_smoker"),
    ), holdout=True),
    Table("doctors", (
        _id(), _str("doctor_name", "Dr. Somchai", "Dr. Patel", "Dr. Nguyen", "Dr. Smith", "Dr. Tanaka",
                    "Dr. Müller"),
        _cat("specialty", "Cardiology", "Pediatrics", "Orthopedics", "Dermatology", "General", nullable=False),
        _cat("hospital", "City Hospital", "St. Mary", "Bumrungrad", "General Clinic"),
        _num("years_experience", 1, 35, "int"),
    ), holdout=True),
    Table("appointments", (
        _id(), _fk("patient_id", "patients"), _fk("doctor_id", "doctors", nullable=False),
        _date("appt_date", nullable=False), _cat("appt_type", "checkup", "follow_up", "emergency", "surgery"),
        _num("fee", 20, 3000), _cat("status", "BOOKED", "DONE", "NO_SHOW", "CANCELLED", nullable=False),
        _num("wait_minutes", 0, 180, "int"),
    ), holdout=True),
    Table("flights", (
        _id(), _cat("airline", "Thai Airways", "Singapore Air", "ANA", "Lufthansa", "Delta", nullable=False),
        _cat("origin", "BKK", "SIN", "NRT", "FRA", "JFK", nullable=False),
        _cat("destination", "BKK", "SIN", "NRT", "FRA", "JFK", "LHR"), _ts("departure_time"),
        _num("distance_km", 300, 15000, "int"), _num("base_fare", 50, 2500),
    ), holdout=True),
    Table("bookings", (
        _id(), _fk("flight_id", "flights", nullable=False), _str("passenger_name", *PERSON_NAMES),
        _cat("seat_class", "ECONOMY", "PREMIUM", "BUSINESS", "FIRST", nullable=False), _num("price", 50, 9000),
        _ts("booked_at"), _bool("is_cancelled"), _cat("loyalty_tier", "NONE", "SILVER", "GOLD", "PLATINUM"),
    ), holdout=True),
)


# ---------------------------------------------------------------------------
# v4: nested data and dense time series (used only by advanced.py)
# ---------------------------------------------------------------------------
def _nested(name: str, kind: str, dtype: str, nullable: bool = True) -> Column:
    return Column(name, kind, dtype=dtype, nullable=nullable)


def _dense_ts(rng: random.Random, start: datetime, n: int, max_gap_min: int) -> list[datetime]:
    """Increasing timestamps with gaps of 1..max_gap_min minutes (so session / interval logic has something
    to find), returned in shuffled order."""
    t, out = start, []
    for _ in range(n):
        t += timedelta(minutes=rng.choice([rng.randint(1, 20), rng.randint(1, max_gap_min)]))
        out.append(t)
    rng.shuffle(out)
    return out


CART_ITEM = "STRUCT<product_id: INT, qty: INT, price: DOUBLE, promo: STRING>"
PROMO_CODES = ("SPRING10", "VIP", "BUNDLE", "WELCOME5")


def _cart_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-carts")
    times = _dense_ts(rng, datetime(2025, 3, 1, 8, 0), 40, 600)
    rows = []
    for i in range(1, 41):
        items = [(rng.randint(1, 12), rng.randint(1, 6), round(rng.uniform(5, 300), 2),
                  rng.choice([None, None, *PROMO_CODES])) for _ in range(rng.choice([0, 1, 2, 2, 3, 4]))]
        ship = None if rng.random() < 0.06 else (rng.choice(CITIES), f"{rng.randint(10000, 99999)}",
                                                  rng.choice([*COUNTRIES, None]))
        attrs = {}
        for k, pool in (("device", ("ios", "android", "web")), ("utm_source", ("google", "email", "tiktok")),
                        ("gift", ("true", "false")), ("weight_kg", ("0.5", "1.25", "3", "n/a"))):
            if rng.random() < 0.7:
                attrs[k] = rng.choice(pool)
        rows.append((i, rng.randint(1, 45) if rng.random() > 0.05 else None, times[i - 1], items,
                     rng.sample(PROMO_CODES, rng.randint(0, 2)), ship, attrs))
    return rows


def _app_log_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-app_logs")
    times = _dense_ts(rng, datetime(2025, 6, 1, 0, 0), 40, 240)
    rows = []
    for i in range(1, 41):
        r = rng.random()
        if r < 0.05:
            payload = None
        elif r < 0.1:
            payload = "n/a"
        else:
            p: dict = {"user": {"id": rng.randint(1, 30), "tier": rng.choice(["free", "gold", "pro"])}}
            if rng.random() < 0.85:
                p["duration"] = round(rng.uniform(0.05, 4.0), 2)
            if rng.random() < 0.7:
                p["retries"] = rng.randint(0, 3)
            p["tags"] = rng.sample(["mobile", "beta", "eu", "cache_hit", "slow"], rng.randint(0, 3))
            payload = json.dumps(p)
        ctx = {k: rng.choice(pool) for k, pool in (("region", ("ap-southeast", "us-east", "eu-west")),
                                                   ("version", ("1.4.0", "1.5.2", "2.0.0")),
                                                   ("trace", ("on", "off"))) if rng.random() < 0.75}
        rows.append((i, rng.choice(["auth", "billing", "search", "checkout"]), rng.choice(["h1", "h2", "h3", "h4"]),
                     times[i - 1], rng.choice(["INFO", "INFO", "WARN", "ERROR", "DEBUG"]),
                     rng.randint(1, 3000) if rng.random() > 0.08 else None,
                     rng.choice([200, 200, 201, 400, 404, 500, 503]), payload, ctx))
    return rows


def _price_tier_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-price_tiers")
    rows = []
    for i in range(1, 41):
        n = rng.randint(2, 4)
        limits = sorted(rng.sample([50, 100, 500, 1000, 5000, 10000], n))
        prices = sorted((round(rng.uniform(0.5, 40), 2) for _ in range(n)), reverse=True)
        discounts = [round(rng.choice([0.0, 0.05, 0.1, 0.15, 0.25]), 2) for _ in range(n)]
        rows.append((i, rng.choice(["starter", "basic", "pro", "enterprise"]), limits, prices, discounts,
                     date(2024, 1, 1) + timedelta(days=rng.randint(0, 700)), rng.choice(["USD", "EUR", "THB"])))
    return rows


def _device_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-device_readings")
    times = _dense_ts(rng, datetime(2025, 9, 1, 0, 0), 40, 180)
    rows = []
    for i in range(1, 41):
        def val(lo, hi):
            return None if rng.random() < 0.1 else round(rng.uniform(lo, hi), 2)
        rows.append((i, rng.choice(["DV-01", "DV-02", "DV-03", "DV-04"]), times[i - 1], val(15, 45), val(20, 90),
                     val(980, 1040), rng.randint(0, 15), rng.choice(["1.0.3", "1.1.0", "2.0.1"])))
    return rows


def _promotion_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-promotions")
    rows = []
    for i in range(1, 13):
        start = date(2023, 1, 1) + timedelta(days=rng.randint(0, 3 * 365 - 90))
        rows.append((i, rng.choice(["web", "mobile", "store", "partner", None]), rng.choice(PROMO_CODES),
                     start, start + timedelta(days=rng.randint(7, 90)), round(rng.uniform(0.05, 0.4), 2)))
    return rows


TRIP_STOP = "STRUCT<stop_name: STRING, wait_min: INT, fare: DOUBLE>"


def _trip_rows(seed: int) -> list[tuple]:
    rng = random.Random(f"{seed}-vehicle_trips")
    times = _dense_ts(rng, datetime(2025, 11, 3, 6, 0), 40, 300)
    rows = []
    for i in range(1, 41):
        stops = [(rng.choice(["Central", "Airport", "Old Town", "Harbor", "Uni", "Mall"]), rng.randint(0, 25),
                  round(rng.uniform(2, 60), 2)) for _ in range(rng.choice([0, 1, 2, 3]))]
        tele = None if rng.random() < 0.07 else json.dumps(
            {"speed": {"avg": round(rng.uniform(15, 70), 1), "max": round(rng.uniform(70, 130), 1)},
             "battery": rng.randint(5, 100)})
        n = rng.randint(1, 3)
        rows.append((i, rng.randint(1, 10), times[i - 1], stops,
                     (rng.choice(["BKK", "DMK", "CNX"]), rng.choice(["BKK", "DMK", "CNX", "HKT", None])), tele,
                     {k: rng.choice(v) for k, v in (("vehicle_type", ("sedan", "van", "ev")),
                                                    ("rating", ("4.5", "5", "3.8", "none"))) if rng.random() < 0.8},
                     [round(rng.uniform(1, 30), 2) for _ in range(n)], [rng.choice([1.0, 1.1, 1.25]) for _ in range(n)]))
    return rows


EXTRA_TABLES: tuple[Table, ...] = (
    Table("carts", (
        _id(), _fk("customer_id", "customers"), Column("created_at", "ts", nullable=False),
        _nested("items", "array_struct", f"ARRAY<{CART_ITEM}>", nullable=False),
        Column("coupon_codes", "array", pool=PROMO_CODES, nullable=False),
        _nested("ship_to", "struct", "STRUCT<city: STRING, zip: STRING, country: STRING>"),
        _nested("attributes", "map", "MAP<STRING, STRING>", nullable=False),
    ), rows=_cart_rows),
    Table("app_logs", (
        _id(), _cat("service", "auth", "billing", "search", "checkout", nullable=False),
        _cat("host", "h1", "h2", "h3", "h4", nullable=False), Column("log_ts", "ts", nullable=False),
        _cat("level", "INFO", "WARN", "ERROR", "DEBUG", nullable=False), _num("latency_ms", 1, 3000, "int"),
        Column("status_code", "int", lo=200, hi=503, nullable=False),
        _nested("payload_json", "json", "STRING"),
        _nested("context", "map", "MAP<STRING, STRING>", nullable=False),
    ), rows=_app_log_rows),
    Table("price_tiers", (
        _id(), _cat("plan", "starter", "basic", "pro", "enterprise", nullable=False),
        _nested("tier_limits", "array_int", "ARRAY<INT>", nullable=False),
        _nested("tier_prices", "array_double", "ARRAY<DOUBLE>", nullable=False),
        _nested("discounts", "array_double", "ARRAY<DOUBLE>", nullable=False),
        _date("effective_from", nullable=False), _cat("currency", "USD", "EUR", "THB", nullable=False),
    ), rows=_price_tier_rows),
    Table("device_readings", (
        _id(), _cat("device_code", "DV-01", "DV-02", "DV-03", "DV-04", nullable=False),
        Column("reading_ts", "ts", nullable=False), _num("temp_c", 15, 45), _num("humidity", 20, 90),
        _num("pressure", 980, 1040), Column("status_bits", "int", lo=0, hi=15, nullable=False),
        _cat("firmware", "1.0.3", "1.1.0", "2.0.1", nullable=False),
    ), rows=_device_rows),
    Table("promotions", (
        Column("id", "id", lo=1, hi=12, nullable=False), _cat("channel", "web", "mobile", "store", "partner"),
        _cat("promo_code", *PROMO_CODES, nullable=False), _date("starts_on", nullable=False),
        _date("ends_on", nullable=False), _num("discount_pct", 0.05, 0.4, nullable=False),
    ), rows=_promotion_rows),
    # ---- hold-out: nested data, never used by the generator ----
    Table("vehicle_trips", (
        _id(), _num("driver_id", 1, 10, "int", nullable=False), Column("trip_start", "ts", nullable=False),
        _nested("stops", "array_struct", f"ARRAY<{TRIP_STOP}>", nullable=False),
        _nested("route", "struct", "STRUCT<origin: STRING, destination: STRING>", nullable=False),
        _nested("telemetry_json", "json", "STRING"),
        _nested("extras", "map", "MAP<STRING, STRING>", nullable=False),
        _nested("fare_components", "array_double", "ARRAY<DOUBLE>", nullable=False),
        _nested("surcharge_rates", "array_double", "ARRAY<DOUBLE>", nullable=False),
    ), holdout=True, rows=_trip_rows),
)

TABLE_BY_NAME = {t.name: t for t in TABLES + EXTRA_TABLES}


def train_tables() -> list[Table]:
    return [t for t in TABLES if not t.holdout]


def holdout_tables() -> list[Table]:
    return [t for t in TABLES + EXTRA_TABLES if t.holdout]


def extra_train_tables() -> list[Table]:
    return [t for t in EXTRA_TABLES if not t.holdout]


# --------------------------------------------------------------------------
# Synthetic rows for execution checks
# --------------------------------------------------------------------------
N_ROWS = 40
DATE_LO = date(2023, 1, 1)
DATE_SPAN_DAYS = 3 * 365


def _value(col: Column, rng: random.Random, row_id: int):
    if col.kind == "id":
        return row_id
    if col.nullable and rng.random() < 0.08:
        return None
    if col.kind == "fk":
        # a few dangling keys so anti-joins / outer joins have something to find
        hi = int(col.hi)
        return rng.randint(1, hi) if rng.random() < 0.92 else rng.randint(hi + 1, hi + 5)
    if col.kind == "int":
        return rng.randint(int(col.lo), int(col.hi))
    if col.kind == "double":
        return round(rng.uniform(col.lo, col.hi), 2)
    if col.kind in ("cat", "str"):
        return rng.choice(col.pool)
    if col.kind == "date":
        return DATE_LO + timedelta(days=rng.randint(0, DATE_SPAN_DAYS))
    if col.kind == "ts":
        return datetime(2023, 1, 1) + timedelta(minutes=rng.randint(0, DATE_SPAN_DAYS * 24 * 60))
    if col.kind == "bool":
        return rng.random() < 0.5
    if col.kind == "array":
        return rng.sample(col.pool, rng.randint(0, 3))
    raise ValueError(col.kind)


def make_rows(table: Table, seed: int = 7) -> list[tuple]:
    if table.rows is not None:
        return table.rows(seed)
    rng = random.Random(f"{seed}-{table.name}")
    return [tuple(_value(c, rng, i) for c in table.columns) for i in range(1, N_ROWS + 1)]


def make_dataframes(spark, seed: int = 7) -> dict:
    """Return {"df_<table>": DataFrame} and register each table as a temp view."""
    frames = {}
    for t in TABLES + EXTRA_TABLES:
        df = spark.createDataFrame(make_rows(t, seed), t.ddl)
        df.createOrReplaceTempView(t.name)
        frames[f"df_{t.name}"] = df
    return frames
