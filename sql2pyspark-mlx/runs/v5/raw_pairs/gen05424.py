from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "FAILED"]) & (F.col("amount") < 780))
    .select(
        "order_id",
        F.when(F.col("status").isin(["REFUNDED", "FAILED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
