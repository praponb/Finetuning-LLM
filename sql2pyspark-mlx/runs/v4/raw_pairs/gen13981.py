from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("status", ["SUCCESS", "FAILED"])
    .agg(F.max("amount"))
    .select(
        "order_id",
        F.col("SUCCESS").alias("success"),
        F.col("FAILED").alias("failed"),
    )
)
