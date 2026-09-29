from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select("status", F.round("amount", 0).alias("amount_rounded"))
)
