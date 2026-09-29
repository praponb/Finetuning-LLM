from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "merchant")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 49)
    .orderBy(F.col("amount_range").desc())
)
