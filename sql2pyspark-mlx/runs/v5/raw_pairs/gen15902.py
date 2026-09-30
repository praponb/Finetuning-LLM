from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(890, 1530) & F.hour("paid_at").between(10, 14))
    .select("status", "method", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
    .orderBy("amount_filled")
)
