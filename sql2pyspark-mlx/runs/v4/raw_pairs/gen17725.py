from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("paid_at_year", F.year("paid_at"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.discount"))
)

base = (
    joined
    .filter(F.col("paid_at") >= F.lit("2023-08-15 01:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "paid_at_year",
        "discount",
        F.ceil("paid_at_year").alias("paid_at_year_whole"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.channel"),
        F.col("d.status").alias("order_status"),
        F.col("d.shipping_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_status",
        F.col("discount").alias("DISCOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        "amount",
    )
    .unpivot(
        ["id", "order_status"],
        ["DISCOUNT", "SHIPPING_FEE_VAL", "amount"],
        "kpi",
        "metric_value",
    )
)
