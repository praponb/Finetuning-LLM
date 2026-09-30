from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") < 630) & (F.col("method") == "cash"))
    .select("id", "paid_at", F.floor("amount").alias("amount_whole"))
)
