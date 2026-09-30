from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .groupBy("merchant", "txn_type")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 2300)
    .select("merchant", "txn_type", "max_amount")
    .orderBy(F.col("max_amount").desc(), "txn_type", "merchant")
    .limit(50)
)
