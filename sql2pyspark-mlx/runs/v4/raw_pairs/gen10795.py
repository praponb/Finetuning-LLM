from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        F.round((F.col("amount") * 0.85), 2).alias("amount_scaled"),
    )
)

result = (
    cleaned
    .groupBy("order_id")
    .pivot("method", ["cash", "bank_transfer"])
    .agg(F.sum("amount_scaled").alias("amount"), F.count(F.lit(1)).alias("n"))
)
