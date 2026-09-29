from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-10-01") & (F.col("amount") <= 3900))
    .select("is_flagged", F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"))
)
