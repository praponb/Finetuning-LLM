from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-12 00:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

kept_keys = (
    cleaned
    .select("order_id", "status")
    .exceptAll(
        df_payments
        .filter(F.col("amount") <= 790)
        .select("order_id", "status")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.order_date"), F.col("b.status").alias("order_status"))
)

joined_payments = (
    joined
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.store_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.amount_filled",
        "src.status_method_key",
        "src.method_clean",
        "src.channel",
        "src.order_date",
        "src.order_status",
        F.col("p.shipping_fee"),
    )
)

remaining_keys = (
    joined_payments
    .select("id", "order_id")
    .subtract(
        df_payments
        .filter(F.col("paid_at") < "2024-04-11")
        .select("id", "order_id")
    )
)

reconciled_payments = (
    joined_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

pivoted = (
    reconciled_payments
    .groupBy("order_id")
    .pivot("channel", ["mobile", "partner", "web"])
    .agg(F.max("shipping_fee"))
)

result = (
    pivoted
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("order_id", "mobile", "partner", "web", F.col("sum_order_item_quantity"))
    .orderBy(F.col("order_id").desc_nulls_first(), "mobile")
    .limit(10)
)
