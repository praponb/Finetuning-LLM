from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.date_add("shipped_date", 76).alias("shipped_date_plus_76d"),
        F.when(F.col("status").isin(["DELIVERED", "RETURNED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        "src.status",
        "src.shipped_date_plus_76d",
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
        F.col("o.amount"),
    )
)
