from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "bank_transfer") & (F.col("status") != "FAILED"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy("avg_amount")
)
