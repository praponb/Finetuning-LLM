from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-05-14")))
    .select(
        "*",
        F.round((F.col("amount") * 1.1), 2).alias("adjusted_amount"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.adjusted_amount",
        "e.method_filled",
        F.col("b.shipping_fee"),
        F.col("b.channel"),
        F.col("b.order_date"),
    )
)
