from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 11)
    .select(
        "id",
        "amount",
        "order_id",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)
