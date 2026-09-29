from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("amount_range") < 70)
    .orderBy(F.col("order_month").desc(), F.col("stddev_discount").desc())
    .limit(5)
)
