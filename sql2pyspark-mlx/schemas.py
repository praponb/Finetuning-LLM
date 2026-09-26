"""Shared table catalog for the SQL -> PySpark dataset.

Used by generate_dataset.py (to build queries), verify_pairs.py (to build small
seeded DataFrames and execute both sides) and eval/evaluate.py.

Tables flagged ``holdout=True`` are never used by the generator; they only
appear in the hand-written evaluation set so we can measure generalisation to
unseen table/column names.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta

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

    def col(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)

    @property
    def ddl(self) -> str:
        return ", ".join(f"{c.name} {SPARK_TYPES[c.kind]}" for c in self.columns)


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

TABLE_BY_NAME = {t.name: t for t in TABLES}


def train_tables() -> list[Table]:
    return [t for t in TABLES if not t.holdout]


def holdout_tables() -> list[Table]:
    return [t for t in TABLES if t.holdout]


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
    rng = random.Random(f"{seed}-{table.name}")
    return [tuple(_value(c, rng, i) for c in table.columns) for i in range(1, N_ROWS + 1)]


def make_dataframes(spark, seed: int = 7) -> dict:
    """Return {"df_<table>": DataFrame} and register each table as a temp view."""
    frames = {}
    for t in TABLES:
        df = spark.createDataFrame(make_rows(t, seed), t.ddl)
        df.createOrReplaceTempView(t.name)
        frames[f"df_{t.name}"] = df
    return frames
