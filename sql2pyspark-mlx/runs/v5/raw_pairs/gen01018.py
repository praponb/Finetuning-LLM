from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("status", F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"))
    .orderBy(F.col("status").desc())
)
