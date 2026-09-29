from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy("amount_range", "is_flagged")
    .limit(1)
)
