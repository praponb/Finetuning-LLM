from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("country", "account_type").orderBy(F.col("opened_date").desc(), "id")
w_running = Window.partitionBy("account_type", "country").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id")

enriched = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.opened_date",
        "src.status",
        F.col("c.country"),
    )
)

enriched_accounts = (
    enriched
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w_running).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

matched_keys = (
    enriched_accounts
    .select("id", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("balance").isNotNull())
        .select("id", "customer_id")
    )
)

reconciled = (
    enriched_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.least("row_seq", "seq_no").alias("least_row_seq_seq_no"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.segment").alias("segment"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.row_seq")).alias("max_row_seq"),
    )
)
