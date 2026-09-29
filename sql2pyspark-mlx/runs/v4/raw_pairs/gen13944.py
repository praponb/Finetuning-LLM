from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) | F.col("order_id").isin([19, 26, 28, 32])))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
