from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("method").desc())
    .limit(5)
)
