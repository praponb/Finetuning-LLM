from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.status",
        F.col("o.channel"),
    )
)
