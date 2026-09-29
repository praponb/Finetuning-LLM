from pyspark.sql import functions as F

result = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.status",
        F.col("o.shipping_fee"),
    )
)
