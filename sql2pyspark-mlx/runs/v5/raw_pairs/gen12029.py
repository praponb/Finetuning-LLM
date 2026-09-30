from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy("status", F.col("amount_range").desc())
)
