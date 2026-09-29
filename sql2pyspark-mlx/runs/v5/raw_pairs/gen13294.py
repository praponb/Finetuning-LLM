from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1360) | (F.col("status") == "REFUNDED"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.countDistinct("amount").alias("unique_methods"),
    )
    .orderBy(F.col("unique_methods").desc())
)
