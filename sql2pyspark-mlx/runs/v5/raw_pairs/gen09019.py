from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-08-24")))
    .withColumn("method_upper", F.upper("method"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

remaining_keys = (
    base
    .select("order_id")
    .subtract(
        df_payments
        .filter(F.col("paid_at") >= "2025-01-24")
        .select("order_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.amount") > 550)
    .select(
        "s.order_id",
        "s.amount",
        "s.paid_at",
        "s.method_upper",
        "s.amount_filled",
        "s.resolved_method",
        F.col("d.discount"),
    )
)

with_windows = (
    joined
    .select(
        "order_id",
        "resolved_method",
        F.percent_rank().over(w).alias("amount_percent_rank"),
    )
)

result = (
    with_windows
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "resolved_method",
        "amount_percent_rank",
        F.col("min_order_item_unit_price"),
    )
)
