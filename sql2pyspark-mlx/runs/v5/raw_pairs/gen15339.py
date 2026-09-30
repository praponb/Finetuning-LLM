from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-06-09 13:00:00").cast("timestamp"))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

cleaned_payments = (
    cleaned
    .filter((F.col("paid_at") >= F.lit("2023-08-11").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("method_filled", F.coalesce("method", F.lit("OTHER")))
)

with_dims = (
    cleaned_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.status",
        "e.status_title",
        "e.method_filled",
        F.col("p.discount"),
        F.col("p.channel"),
        F.col("p.shipping_fee"),
    )
)

with_dims_payments = (
    with_dims
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.channel").isin(["partner", "store"]))
    .select("a.*", F.col("o.amount").alias("order_amount"), F.col("o.order_date"))
)

result = (
    with_dims_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_status", "payment_channel"], ["order_status"], []],
        F.col("p.status").alias("order_status"),
        F.col("e.channel").alias("payment_channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.shipping_fee")).alias("max_shipping_fee"),
        F.max(F.col("e.discount")).alias("max_discount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
