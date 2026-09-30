from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("account_id").isin([9, 40]) & (F.col("account_id") == 36)) | F.col("merchant").isin(["Shell", "Grab"]))
)

agg = (
    base
    .groupBy("merchant", "is_flagged")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    agg
    .filter(F.col("max_amount") >= 2200)
    .select("merchant", "is_flagged")
    .orderBy("merchant")
)
