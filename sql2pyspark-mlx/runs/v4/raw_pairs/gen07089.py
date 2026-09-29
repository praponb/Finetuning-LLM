from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy(F.col("amount_range").desc())
)
