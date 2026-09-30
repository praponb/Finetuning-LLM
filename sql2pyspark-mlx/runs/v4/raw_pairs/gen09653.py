from pyspark.sql import functions as F

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.order_date"),
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.discount").alias("order_discount"),
        F.col("p.order_date").alias("order_order_date"),
        F.col("p.status").alias("order_status_2"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("amount").between(660, 960))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "order_date",
        "order_status",
        "discount",
        "order_discount",
        "order_status_2",
        F.add_months("order_order_date", 6).alias("order_order_date_plus_months"),
        F.greatest("discount", "amount").alias("greatest_discount_amount"),
    )
)

kept_keys = (
    cleaned
    .select("status", "method")
    .intersect(
        df_payments
        .filter(F.col("amount") > 560)
        .select("status", "method")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "method",
        "amount",
        "status",
        "order_date",
        "order_status",
        "discount",
        "order_discount",
    )
    .orderBy("order_discount", F.col("discount").desc())
)
