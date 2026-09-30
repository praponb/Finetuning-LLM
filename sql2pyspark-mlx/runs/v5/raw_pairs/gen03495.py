from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
