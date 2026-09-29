from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-12-30")))
    .select("*", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)

result = (
    base
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "Lazada"])
    .agg(F.max("amount_filled").alias("amount"))
)
