from pyspark.sql import functions as F

result = (
    df_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        F.col("d.amount").alias("order_amount"),
    )
)
