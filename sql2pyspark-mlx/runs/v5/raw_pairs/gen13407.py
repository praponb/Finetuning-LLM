from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.avg("discount").alias("avg_discount"),
    )
    .orderBy(F.col("amount_range").desc())
)
